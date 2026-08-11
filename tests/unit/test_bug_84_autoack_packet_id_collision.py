"""Reproduction for GitHub issue #84.

Auto-ACK inbound QoS 1 currently accepts a packet identifier that is still
owned by an unfinished inbound QoS 2 exchange. The manual_ack path already
refuses this collision in ``test_rc_regressions.py``; the default auto-ACK
path must do the same.
"""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
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
    remaining = b"\x00\x00"
    if engine.config.protocol is MQTTProtocolVersion.MQTTv5:
        remaining += b"\x00"
    _feed(engine, encode_frame(PacketType.CONNACK, 0, remaining))
    engine.take_effects()


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/84 — auto-ACK skips inbound mid collision check",
)
def test_autoack_qos1_over_qos2_packet_id_is_refused() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="bug84",
            protocol=MQTTProtocolVersion.MQTTv5,
            manual_ack=False,
        ),
        MemoryInflightStore(),
    )
    _connect(engine)

    _feed(
        engine,
        PublishPacket(
            topic="collide/topic",
            payload=b"qos2-holder",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            dup=False,
            mid=7,
        ).encode(MQTTProtocolVersion.MQTTv5),
    )
    engine.take_effects()
    before = engine.store.get_in(7)
    assert before is not None
    assert before.qos is QoS.EXACTLY_ONCE

    _feed(
        engine,
        PublishPacket(
            topic="collide/topic",
            payload=b"q1-collide",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            dup=False,
            mid=7,
        ).encode(MQTTProtocolVersion.MQTTv5),
    )
    effects = engine.take_effects()
    after = engine.store.get_in(7)

    assert after is not None
    assert after.qos is QoS.EXACTLY_ONCE
    assert after.payload == b"qos2-holder"
    assert not any(
        e.kind is EffectKind.MESSAGE and e.data.payload == b"q1-collide" for e in effects
    )
    disconnects = [e.data for e in effects if e.kind is EffectKind.DISCONNECTED]
    assert disconnects, "packet id collision must disconnect"
    assert disconnects[0].reason_code == 0x82
    assert engine.state is ConnectionState.DISCONNECTED
