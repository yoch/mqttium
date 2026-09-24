"""Receive Maximum must not delay PUBREL during resumed-session replay."""

from __future__ import annotations

from tests.support import stored_record

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import MQTTProtocolVersion, OutboundQoSState, PacketType, QoS
from mqttium.packets import encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import OutboundMessage, Properties


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def test_resumed_session_replays_pubrel_when_send_quota_is_exhausted() -> None:
    store = MemoryInflightStore()
    store.put_out(
        stored_record(
            OutboundMessage(
                mid=1,
                topic="t/1",
                payload=b"publish",
                qos=QoS.EXACTLY_ONCE,
                retain=False,
                state=OutboundQoSState.WAIT_PUBREC,
            )
        )
    )
    store.put_out(
        stored_record(
            OutboundMessage(
                mid=2,
                topic="",
                payload=b"",
                qos=QoS.EXACTLY_ONCE,
                retain=False,
                state=OutboundQoSState.WAIT_PUBCOMP,
                logical_size=15,  # charge retained from the original publication
            )
        )
    )

    engine = ProtocolEngine(
        EngineConfig(
            client_id="quota-replay",
            protocol=MQTTProtocolVersion.MQTTv5,
            clean_start=False,
        ),
        store,
    )
    engine.begin_connect()

    connack_props = Properties({"receive_maximum": 1})
    body = bytearray((0x01, 0x00))
    body.extend(encode_properties(connack_props, "CONNACK"))
    _feed(engine, encode_frame(PacketType.CONNACK, 0, bytes(body)))

    sends = [effect.data for effect in engine.take_effects() if effect.kind is EffectKind.SEND]
    packet_types = [(item if isinstance(item, bytes) else item[0])[0] & 0xF0 for item in sends]

    assert PacketType.PUBLISH.value in packet_types
    assert PacketType.PUBREL.value in packet_types
    assert engine.flow.inflight == 1

    # The replayed PUBREL never took a slot on this connection, so its PUBCOMP
    # releases none: mid 1's PUBLISH still holds the only one (#545). Crediting
    # it, as the literal §4.9 counter would, lets a second PUBLISH out while
    # mid 1 is still unacknowledged on this connection.
    _feed(engine, encode_frame(PacketType.PUBCOMP, 0, b"\x00\x02"))
    engine.take_effects()
    assert engine.flow.inflight == 1
    _feed(engine, encode_frame(PacketType.PUBREC, 0, b"\x00\x01"))
    _feed(engine, encode_frame(PacketType.PUBCOMP, 0, b"\x00\x01"))
    engine.take_effects()
    assert engine.flow.inflight == 0
