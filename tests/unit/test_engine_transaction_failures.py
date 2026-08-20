"""Protocol engine state legality, transaction rollback, and QoS 2 replay."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.enums import (
    ConnectionState,
    MQTTProtocolVersion,
    OutboundQoSState,
    PacketType,
    QoS,
)
from mqttium.packets import PubAckPacket, PubCompPacket, PublishPacket
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine
from mqttium.types import OutboundMessage, OutboundRecordMeta


def _raw(wire: bytes) -> RawPacket:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    packet = decoder.next_packet()
    assert packet is not None
    return packet


def _sent_packet_types(effects) -> list[PacketType]:
    result: list[PacketType] = []
    for effect in effects:
        if effect.kind is not EffectKind.SEND:
            continue
        data = effect.data
        wire = data if isinstance(data, bytes) else data[0] + data[1]
        result.append(_raw(wire).packet_type)
    return result


def test_rejects_packets_outside_connection_state() -> None:
    engine = ProtocolEngine()
    publish = PublishPacket(
        topic="state/test",
        payload=b"x",
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
    )
    engine.handle_raw(_raw(publish.encode()))
    effects = engine.take_effects()
    assert [effect.kind for effect in effects] == [EffectKind.PROTOCOL_ERROR]
    assert "state=NEW" in effects[0].data

    engine.begin_connect()
    engine.handle_raw(_raw(PubAckPacket(mid=1).encode()))
    effects = engine.take_effects()
    assert [effect.kind for effect in effects] == [EffectKind.PROTOCOL_ERROR]
    assert "PUBACK" in effects[0].data
    assert "CONNECTING" in effects[0].data


class _FailingPutStore(MemoryInflightStore):
    def put_out(self, msg: OutboundMessage) -> None:
        raise RuntimeError("store write failed")


def test_direct_launch_rolls_back_mid_and_flow_on_store_failure() -> None:
    store = _FailingPutStore()
    engine = ProtocolEngine(store=store)
    engine.state = ConnectionState.CONNECTED

    with pytest.raises(RuntimeError, match="store write failed"):
        engine.queue_publish("rollback/direct", b"x", qos=1)

    assert engine.flow.inflight == 0
    assert len(engine.packet_ids) == 0
    assert list(store.out_items()) == []
    assert not engine.take_effects()


class _FailOnTransitionStore(MemoryInflightStore):
    fail_transition = False

    def transition_out(
        self,
        mid: int,
        expected_state: OutboundQoSState,
        new_state: OutboundQoSState,
        *,
        compact: bool = False,
    ) -> OutboundRecordMeta | None:
        if self.fail_transition and expected_state is OutboundQoSState.QUEUED:
            raise RuntimeError("transition write failed")
        return super().transition_out(mid, expected_state, new_state, compact=compact)


def test_queued_launch_failure_releases_resources_and_emits_failure() -> None:
    store = _FailOnTransitionStore()
    engine = ProtocolEngine(
        config=EngineConfig(max_outbound_inflight=1),
        store=store,
    )
    engine.state = ConnectionState.CONNECTED
    engine.flow.apply_broker_receive_maximum(65535, 1)

    first = engine.queue_publish("rollback/first", b"1", qos=1)
    second = engine.queue_publish("rollback/second", b"2", qos=1)
    assert first.mid is not None and second.mid is not None
    assert engine.flow.inflight == 1
    engine.take_effects()

    store.fail_transition = True
    engine.handle_raw(_raw(PubAckPacket(mid=first.mid).encode()))
    effects = engine.take_effects()

    failures = [effect.data for effect in effects if effect.kind is EffectKind.PUBLISH_FAILED]
    assert len(failures) == 1
    assert failures[0].mid == second.mid
    assert isinstance(failures[0].reason, RuntimeError)
    assert engine.flow.inflight == 0
    assert not engine.packet_ids.in_use(second.mid)
    assert store.get_out(second.mid) is None


def test_qos2_replay_never_exceeds_local_window() -> None:
    store = MemoryInflightStore()
    for mid in (1, 2, 3):
        store.put_out(
            OutboundMessage(
                mid=mid,
                topic=f"replay/{mid}",
                payload=b"x",
                qos=QoS.EXACTLY_ONCE,
                retain=False,
                state=OutboundQoSState.WAIT_PUBCOMP,
            )
        )

    engine = ProtocolEngine(
        config=EngineConfig(
            client_id="qos2-replay",
            clean_start=False,
            protocol=MQTTProtocolVersion.MQTTv311,
            max_outbound_inflight=1,
        ),
        store=store,
    )
    engine.begin_connect()
    engine.handle_raw(
        RawPacket(
            packet_type=PacketType.CONNACK,
            flags=0,
            remaining=b"\x01\x00",
        )
    )
    effects = engine.take_effects()
    assert engine.flow.inflight == 1
    assert _sent_packet_types(effects).count(PacketType.PUBREL) == 1

    for completed_mid, remaining in ((1, 2), (2, 1), (3, 0)):
        engine.handle_raw(_raw(PubCompPacket(mid=completed_mid).encode()))
        effects = engine.take_effects()
        assert engine.flow.inflight == (1 if remaining else 0)
        expected_pubrel = 1 if remaining else 0
        assert _sent_packet_types(effects).count(PacketType.PUBREL) == expected_pubrel

    assert list(store.out_items()) == []
    assert len(engine.packet_ids) == 0


def test_connack_refusal_preserves_reason_for_reconnect_policy() -> None:
    engine = ProtocolEngine()
    engine.begin_connect()
    engine.handle_raw(
        RawPacket(
            packet_type=PacketType.CONNACK,
            flags=0,
            remaining=b"\x00\x05",
        )
    )
    effects = engine.take_effects()
    disconnected = next(effect.data for effect in effects if effect.kind is EffectKind.DISCONNECTED)
    assert disconnected.reason_code == 5
    assert disconnected.from_broker is True
