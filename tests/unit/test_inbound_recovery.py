"""Persistent inbound QoS recovery and receive-window accounting."""

from __future__ import annotations


from mqttium.api.async_client import AsyncClient
from mqttium.enums import (
    ConnectionState,
    InboundQoSState,
    MQTTProtocolVersion,
    PacketType,
    QoS,
)
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.engine import EffectKind, EngineConfig, EngineEffect, ProtocolEngine
from mqttium.types import InboundMessage, Message
from mqttium.codec.buffer import RawPacket


def _inbound(mid: int, *, state=InboundQoSState.WAIT_PUBREL, delivered=True, user_acked=False):
    return InboundMessage(
        mid=mid,
        topic=f"recover/{mid}",
        payload=str(mid).encode(),
        qos=QoS.EXACTLY_ONCE if state is not InboundQoSState.WAIT_PUBACK else QoS.AT_LEAST_ONCE,
        retain=False,
        state=state,
        delivered=delivered,
        user_acked=user_acked,
    )


def _resume(engine: ProtocolEngine) -> list:
    engine.begin_connect()
    engine.take_effects()
    body = b"\x01\x00" + (b"\x00" if engine.config.protocol == MQTTProtocolVersion.MQTTv5 else b"")
    engine.handle_raw(RawPacket(PacketType.CONNACK, 0, body))
    return engine.take_effects()


def test_memory_store_iterates_inbound_in_insertion_order() -> None:
    store = MemoryInflightStore()
    store.put_in(_inbound(3))
    store.put_in(_inbound(1))
    assert [message.mid for message in store.in_items()] == [3, 1]


def test_sqlite_store_iterates_inbound_in_insertion_order(tmp_path) -> None:
    store = SqliteInflightStore(tmp_path / "inflight.sqlite")
    try:
        store.put_in(_inbound(4))
        store.put_in(_inbound(2))
        assert [message.mid for message in store.in_items()] == [4, 2]
    finally:
        store.close()


def test_session_resume_restores_receive_window_count() -> None:
    store = MemoryInflightStore()
    store.put_in(_inbound(1))
    store.put_in(_inbound(2))
    engine = ProtocolEngine(
        EngineConfig(client_id="recovery", clean_start=False, local_receive_maximum=2),
        store=store,
    )

    _resume(engine)

    assert engine.state is ConnectionState.CONNECTED
    assert engine._inbound_inflight == 2


def test_undelivered_qos2_is_replayed_once_after_restart() -> None:
    store = MemoryInflightStore()
    store.put_in(_inbound(7, delivered=False))
    engine = ProtocolEngine(EngineConfig(client_id="recovery", clean_start=False), store=store)

    effects = _resume(engine)
    messages = [effect.data for effect in effects if effect.kind is EffectKind.MESSAGE]

    assert len(messages) == 1
    assert messages[0].mid == 7
    assert messages[0].dup is True
    assert engine._inbound_inflight == 1
    assert engine._recovered_inbound_mids == set()


def test_recovered_manual_ack_is_redelivered_even_if_previously_delivered() -> None:
    store = MemoryInflightStore()
    store.put_in(
        _inbound(
            8,
            state=InboundQoSState.WAIT_USER_ACK,
            delivered=True,
        )
    )
    engine = ProtocolEngine(
        EngineConfig(client_id="recovery", clean_start=False, manual_ack=True),
        store=store,
    )

    effects = _resume(engine)
    messages = [effect.data for effect in effects if effect.kind is EffectKind.MESSAGE]

    assert [message.mid for message in messages] == [8]
    assert messages[0].dup is True


async def test_client_marks_persisted_message_delivered_after_api_delivery() -> None:
    store = MemoryInflightStore()
    store.put_in(_inbound(9, delivered=False))
    client = AsyncClient(
        client_id="mark-delivered",
        store=store,
        message_delivery="iterator",
    )
    message = Message(
        topic="recover/9",
        payload=b"9",
        qos=QoS.EXACTLY_ONCE,
        mid=9,
        dup=True,
    )

    await client._apply_effect(
        EngineEffect(EffectKind.MESSAGE, message),
        nowait=False,
    )

    persisted = store.get_in(9)
    assert persisted is not None and persisted.delivered is True
    queued = client._messages.get_nowait()
    assert isinstance(queued, Message) and queued.mid == 9
