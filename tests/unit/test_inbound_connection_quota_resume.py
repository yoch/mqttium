from __future__ import annotations

from collections.abc import Iterator

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, InboundQoSState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.types import InboundMessage


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connack_v5(*, session_present: bool) -> bytes:
    flags = 1 if session_present else 0
    return encode_frame(PacketType.CONNACK, 0, bytes((flags, 0, 0)))


def _stored_qos2(mid: int, state: InboundQoSState) -> InboundMessage:
    return InboundMessage(
        mid=mid,
        topic=f"resume/{mid}",
        payload=b"persisted",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        state=state,
        delivered=True,
        logical_size=32,
    )


def _stores(tmp_path) -> Iterator[tuple[str, MemoryInflightStore | SqliteInflightStore]]:
    yield "memory", MemoryInflightStore()
    sqlite = SqliteInflightStore(tmp_path / "inflight.sqlite")
    try:
        yield "sqlite", sqlite
    finally:
        sqlite.close()


@pytest.mark.parametrize("state", [InboundQoSState.WAIT_PUBREL, InboundQoSState.WAIT_USER_ACK])
def test_resumed_durable_qos2_does_not_precharge_new_connection_receive_maximum(
    tmp_path, state: InboundQoSState
) -> None:
    for _name, store in _stores(tmp_path):
        store.put_in(_stored_qos2(1, state))
        engine = ProtocolEngine(
            EngineConfig(
                client_id="resume-rm",
                protocol=MQTTProtocolVersion.MQTTv5,
                clean_start=False,
                manual_ack=True,
                max_inbound_inflight=1,
            ),
            store=store,
        )

        engine.begin_connect()
        engine.take_effects()
        _feed(engine, _connack_v5(session_present=True))
        engine.take_effects()

        assert engine.state is ConnectionState.CONNECTED
        assert engine.inbound._inflight == 0

        _feed(
            engine,
            PublishPacket(
                topic="fresh",
                payload=b"x",
                qos=QoS.AT_LEAST_ONCE,
                retain=False,
                dup=False,
                mid=2,
            ).encode(MQTTProtocolVersion.MQTTv5),
        )
        effects = engine.take_effects()

        assert engine.state is ConnectionState.CONNECTED
        assert engine.inbound._inflight == 1
        assert not any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
        assert store.in_meta(1) is not None
        assert store.in_meta(2) is not None


def test_retransmitted_persisted_publish_acquires_one_slot_on_replacement_connection() -> None:
    store = MemoryInflightStore()
    store.put_in(_stored_qos2(1, InboundQoSState.WAIT_PUBREL))
    engine = ProtocolEngine(
        EngineConfig(
            client_id="resume-rm",
            protocol=MQTTProtocolVersion.MQTTv5,
            clean_start=False,
            manual_ack=True,
            max_inbound_inflight=1,
        ),
        store=store,
    )
    engine.begin_connect()
    engine.take_effects()
    _feed(engine, _connack_v5(session_present=True))
    engine.take_effects()

    _feed(
        engine,
        PublishPacket(
            topic="resume/1",
            payload=b"persisted",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            dup=True,
            mid=1,
        ).encode(MQTTProtocolVersion.MQTTv5),
    )
    engine.take_effects()
    assert engine.inbound._inflight == 1

    _feed(
        engine,
        PublishPacket(
            topic="fresh",
            payload=b"x",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            dup=False,
            mid=2,
        ).encode(MQTTProtocolVersion.MQTTv5),
    )
    effects = engine.take_effects()
    assert engine.state is ConnectionState.DISCONNECTED
    error = next(effect.data for effect in effects if effect.kind is EffectKind.PROTOCOL_ERROR)
    assert "Receive Maximum exceeded" in str(error)


def test_completing_old_wait_user_ack_does_not_release_unrelated_current_slot() -> None:
    store = MemoryInflightStore()
    store.put_in(_stored_qos2(1, InboundQoSState.WAIT_USER_ACK))
    engine = ProtocolEngine(
        EngineConfig(
            client_id="resume-rm",
            protocol=MQTTProtocolVersion.MQTTv5,
            clean_start=False,
            manual_ack=True,
            max_inbound_inflight=1,
        ),
        store=store,
    )
    engine.begin_connect()
    engine.take_effects()
    _feed(engine, _connack_v5(session_present=True))
    engine.take_effects()

    _feed(
        engine,
        PublishPacket(
            topic="fresh",
            payload=b"x",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            dup=False,
            mid=2,
        ).encode(MQTTProtocolVersion.MQTTv5),
    )
    engine.take_effects()
    assert engine.inbound._inflight == 1

    engine.ack(1)
    engine.take_effects()

    assert store.in_meta(1) is None
    assert store.in_meta(2) is not None
    assert engine.inbound._inflight == 1
