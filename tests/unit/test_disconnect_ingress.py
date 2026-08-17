"""Ingress behavior while an intentional disconnect is in progress."""

from __future__ import annotations

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connected_engine() -> ProtocolEngine:
    engine = ProtocolEngine(EngineConfig(client_id="disconnect-ingress"))
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
    engine.take_effects()
    return engine


def test_disconnecting_drops_inflight_publish_without_effects() -> None:
    engine = _connected_engine()
    engine.begin_disconnect()
    assert engine.state is ConnectionState.DISCONNECTING

    _feed(
        engine,
        PublishPacket(
            topic="busy/topic",
            payload=b"still in flight",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            dup=False,
            mid=7,
        ).encode(),
    )

    assert engine.state is ConnectionState.DISCONNECTING
    assert engine.take_effects() == []


def test_disconnected_state_ignores_inbound_packets() -> None:
    """Trailing buffered packets after a terminal state must not raise.

    After a broker DISCONNECT (or a refused CONNACK), the read loop may still
    decode packets the peer had already sent. Surfacing them as
    PROTOCOL_ERROR made the runtime overwrite `_disconnect_exc` with an
    "Unexpected X while state=DISCONNECTED" noise error, masking the real
    disconnect reason. They are transport noise, exactly like the packets
    ignored while DISCONNECTING.
    """
    engine = _connected_engine()
    _feed(engine, encode_frame(PacketType.DISCONNECT, 0, b""))
    assert engine.state is ConnectionState.DISCONNECTED

    _feed(engine, encode_frame(PacketType.PINGRESP, 0, b""))
    effects = engine.take_effects()

    assert not any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)
    assert engine.state is ConnectionState.DISCONNECTED
