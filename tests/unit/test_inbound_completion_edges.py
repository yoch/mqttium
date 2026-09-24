"""Edge paths of inbound exchange ownership and deferred completion."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import RawPacket
from mqttium.enums import ConnectionState, InboundQoSState, PacketType, QoS
from mqttium.packets import PublishPacket
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.types import InboundMessage
from tests.support import feed_engine


def _row(mid: int, qos: QoS, state: InboundQoSState, *, delivered: bool = False):
    return InboundMessage(
        mid=mid,
        topic=f"edge/{mid}",
        payload=b"body",
        qos=qos,
        retain=False,
        state=state,
        delivered=delivered,
        logical_size=12,
    )


def _publish(mid: int, qos: QoS, *, dup: bool = True) -> bytes:
    return PublishPacket(
        topic=f"edge/{mid}", payload=b"body", qos=qos, retain=False, dup=dup, mid=mid
    ).encode()


def _resumed(store: MemoryInflightStore, *, manual_ack: bool = False) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(client_id="edge", clean_start=False, manual_ack=manual_ack), store=store
    )
    engine.begin_connect()
    engine.take_effects()
    engine.handle_raw(RawPacket(PacketType.CONNACK, 0, b"\x01\x00"))
    return engine


def _acks(effects: list, first_byte: int) -> list[int]:
    return [
        int.from_bytes(effect.data[2:4], "big")
        for effect in effects
        if effect.kind is EffectKind.SEND_ACK and effect.data[0] == first_byte
    ]


def test_recovered_qos1_retransmitted_twice_owns_one_slot_and_one_puback() -> None:
    store = MemoryInflightStore()
    store.put_in(_row(4, QoS.AT_LEAST_ONCE, InboundQoSState.WAIT_PUBACK))
    engine = _resumed(store)
    replay = [e for e in engine.take_effects() if e.kind is EffectKind.MESSAGE]
    assert [e.data.mid for e in replay] == [4]

    for _ in range(2):
        feed_engine(engine, _publish(4, QoS.AT_LEAST_ONCE))
        assert _acks(engine.take_effects(), 0x40) == []
    assert engine.inbound._inflight == 1

    engine.mark_inbound_delivered(4, replay[0].exchange_token)
    assert _acks(engine.take_effects(), 0x40) == [4]
    assert store.get_in(4) is None
    assert engine.inbound._inflight == 0


def test_recovered_qos1_completion_failure_releases_its_slot() -> None:
    class _FailingComplete(MemoryInflightStore):
        def complete_in(self, mid: int, expected_state: InboundQoSState) -> object:
            raise OSError("complete failed")

    store = _FailingComplete()
    store.put_in(_row(6, QoS.AT_LEAST_ONCE, InboundQoSState.WAIT_PUBACK, delivered=True))
    engine = _resumed(store)
    engine.take_effects()

    with pytest.raises(OSError):
        feed_engine(engine, _publish(6, QoS.AT_LEAST_ONCE))
    assert engine.inbound._inflight == 0


def test_manual_qos1_duplicate_with_vanished_row_releases_its_slot() -> None:
    class _VanishingRow(MemoryInflightStore):
        def get_in(self, mid: int) -> InboundMessage | None:
            return None

    store = _VanishingRow()
    store.put_in(_row(8, QoS.AT_LEAST_ONCE, InboundQoSState.WAIT_PUBACK))
    engine = ProtocolEngine(
        EngineConfig(client_id="edge", clean_start=False, manual_ack=True), store=store
    )
    engine.state = ConnectionState.CONNECTED
    engine.inbound._stored_inbound = 1

    with pytest.raises(RuntimeError, match="disappeared while redelivering"):
        feed_engine(engine, _publish(8, QoS.AT_LEAST_ONCE))
    assert engine.inbound._inflight == 0
    assert 8 not in engine.inbound._current_persisted_mids


def test_qos2_publish_for_an_unknown_local_state_is_a_local_failure() -> None:
    store = MemoryInflightStore()
    store.put_in(_row(9, QoS.EXACTLY_ONCE, InboundQoSState.DONE))
    engine = ProtocolEngine(EngineConfig(client_id="edge", clean_start=False), store=store)
    engine.state = ConnectionState.CONNECTED
    engine.inbound._stored_inbound = 1

    with pytest.raises(RuntimeError, match="unexpected state"):
        feed_engine(engine, _publish(9, QoS.EXACTLY_ONCE))
    assert engine.take_effects() == []
