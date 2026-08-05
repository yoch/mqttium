"""Restart compatibility when manual acknowledgement policy changes."""

from __future__ import annotations

from mqttium.enums import ConnectionState, InboundQoSState, QoS
from mqttium.packets import PubCompPacket, PubRelPacket
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.types import InboundMessage
from tests.support import feed_engine, write_item_bytes


def test_wait_user_ack_completes_when_reopened_without_manual_ack() -> None:
    store = MemoryInflightStore()
    store.put_in(
        InboundMessage(
            mid=17,
            topic="resume/qos2",
            payload=b"payload",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            state=InboundQoSState.WAIT_USER_ACK,
            delivered=True,
        )
    )
    engine = ProtocolEngine(
        EngineConfig(client_id="manual-ack-resume", manual_ack=False),
        store=store,
    )
    engine.state = ConnectionState.CONNECTED
    engine._inbound_inflight = 1

    feed_engine(engine, PubRelPacket(mid=17).encode())

    sends = [
        write_item_bytes(effect.data)
        for effect in engine.take_effects()
        if effect.kind is EffectKind.SEND
    ]
    assert sends == [PubCompPacket(mid=17).encode()]
    assert store.get_in(17) is None
    assert engine._inbound_inflight == 0
