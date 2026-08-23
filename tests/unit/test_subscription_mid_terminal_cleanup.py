"""Pending SUBSCRIBE/UNSUBSCRIBE identifiers are connection-scoped."""

from __future__ import annotations

from mqttium.codec.buffer import RawPacket
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.protocol.engine import EngineConfig, ProtocolEngine


def _connected_engine() -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="sub-mid-cleanup",
            protocol=MQTTProtocolVersion.MQTTv5,
        )
    )
    engine.state = ConnectionState.CONNECTED
    return engine


def _assert_mid_released(engine: ProtocolEngine, mid: int) -> None:
    assert mid not in engine._pending_sub_requests
    assert not engine.outbound.packet_ids.in_use(mid)

    # With no other packet identifier live, releasing the request resets the
    # allocator frontier and the next request can immediately reuse the MID.
    engine.state = ConnectionState.CONNECTED
    assert engine.queue_subscribe("after/terminal") == mid


def test_broker_disconnect_releases_pending_subscribe_mid_immediately() -> None:
    engine = _connected_engine()
    mid = engine.queue_subscribe("before/disconnect")
    assert engine.outbound.packet_ids.in_use(mid)

    engine.handle_raw(RawPacket(PacketType.DISCONNECT, 0, b""))

    assert engine.state is ConnectionState.DISCONNECTED
    _assert_mid_released(engine, mid)


def test_protocol_disconnect_releases_pending_unsubscribe_mid_immediately() -> None:
    engine = _connected_engine()
    mid = engine.queue_unsubscribe("before/protocol-error")
    assert engine.outbound.packet_ids.in_use(mid)

    # AUTH without a CONNECT authentication_method is a reachable MQTT 5
    # protocol error. The handler calls _protocol_disconnect() before surfacing
    # the protocol-error effect.
    engine.handle_raw(RawPacket(PacketType.AUTH, 0, b""))

    assert engine.state is ConnectionState.DISCONNECTED
    _assert_mid_released(engine, mid)
