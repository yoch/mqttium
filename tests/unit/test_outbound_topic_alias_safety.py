"""Connection-scoped outbound Topic Alias establishment and replay safety."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import CONNACK, encode_properties
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.errors import ProtocolError
from mqttium.packets import PublishPacket, encode_frame
from mqttium.persistence import MemoryInflightStore, SqliteInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Properties

from tests.support import feed_engine, write_item_bytes


def _connack(*, alias_maximum: int = 10, session_present: bool = False) -> bytes:
    connack_properties = Properties()
    connack_properties.set("topic_alias_maximum", alias_maximum)
    body = bytearray((int(session_present), 0))
    body.extend(encode_properties(connack_properties, CONNACK))
    return encode_frame(PacketType.CONNACK, 0, body)


def _connected_engine(*, max_outbound_inflight: int | None = None) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="alias-safety",
            protocol=MQTTProtocolVersion.MQTTv5,
            clean_start=False,
            max_outbound_inflight=max_outbound_inflight,
        )
    )
    engine.begin_connect()
    feed_engine(engine, _connack())
    engine.take_effects()
    return engine


def _alias(number: int) -> Properties:
    properties = Properties()
    properties.set("topic_alias", number)
    return properties


def _sent_publish(engine: ProtocolEngine) -> PublishPacket:
    effect = next(effect for effect in engine.take_effects() if effect.kind is EffectKind.SEND)
    decoder = IncrementalDecoder()
    decoder.feed(write_item_bytes(effect.data))
    raw = decoder.next_packet()
    assert raw is not None and raw.packet_type is PacketType.PUBLISH
    return PublishPacket.decode(raw.flags, raw.remaining, MQTTProtocolVersion.MQTTv5)


@pytest.mark.parametrize("qos", [0, 1, 2])
def test_empty_outbound_topic_with_alias_is_refused_without_mutation(qos: int) -> None:
    engine = _connected_engine()
    properties = _alias(1)

    with pytest.raises(ProtocolError, match="Unknown outbound topic alias"):
        engine.queue_publish("", b"payload", qos=qos, properties=properties)

    assert engine.take_effects() == []
    assert engine.pending_outbound_messages == 0
    assert len(engine.packet_ids) == 0
    assert tuple(engine.store.out_items()) == ()


def test_nonempty_topic_with_alias_remains_supported() -> None:
    engine = _connected_engine()
    properties = _alias(1)

    engine.queue_publish("canonical/topic", b"payload", qos=0, properties=properties)
    effects = engine.take_effects()

    assert effects


@pytest.mark.parametrize("qos", [0, 1, 2])
def test_established_alias_allows_empty_topic_for_every_qos(qos: int) -> None:
    engine = _connected_engine()
    properties = _alias(1)

    engine.queue_publish("canonical/topic", b"seed", qos=0, properties=properties)
    _sent_publish(engine)
    engine.queue_publish("", b"payload", qos=qos, properties=properties)

    publish = _sent_publish(engine)
    assert publish.topic == ""
    if qos:
        stored = engine.store.get_out(publish.mid or 0)
        assert stored is not None
        assert stored.topic == "canonical/topic"
        assert stored.encoded_publish is None


def test_alias_replacement_changes_empty_topic_resolution() -> None:
    engine = _connected_engine()
    properties = _alias(1)

    engine.queue_publish("first/topic", qos=0, properties=properties)
    _sent_publish(engine)
    engine.queue_publish("second/topic", qos=0, properties=properties)
    _sent_publish(engine)
    engine.queue_publish("", qos=1, properties=properties)

    publish = _sent_publish(engine)
    stored = engine.store.get_out(publish.mid or 0)
    assert publish.topic == ""
    assert stored is not None and stored.topic == "second/topic"


@pytest.mark.parametrize("alias", [0, 11])
@pytest.mark.parametrize("qos", [0, 1, 2])
def test_invalid_outbound_alias_is_rejected_before_mutation(alias: int, qos: int) -> None:
    engine = _connected_engine()

    with pytest.raises(ProtocolError, match="topic_alias"):
        engine.queue_publish("canonical/topic", qos=qos, properties=_alias(alias))

    assert engine.take_effects() == []
    assert engine.pending_outbound_messages == 0
    assert len(engine.packet_ids) == 0


def test_explicit_alias_queued_before_connack_establishes_mapping_when_sent() -> None:
    engine = ProtocolEngine(
        EngineConfig(client_id="queued-alias", protocol=MQTTProtocolVersion.MQTTv5)
    )
    properties = _alias(2)
    engine.queue_publish("queued/topic", qos=1, properties=properties)

    engine.begin_connect()
    feed_engine(engine, _connack(alias_maximum=2))
    first = _sent_publish(engine)
    assert first.topic == "queued/topic"

    engine.queue_publish("", qos=0, properties=properties)
    assert _sent_publish(engine).topic == ""


def test_alias_only_queued_behind_flow_uses_canonical_topic_when_later_sent() -> None:
    engine = _connected_engine(max_outbound_inflight=1)
    properties = _alias(1)
    first = engine.queue_publish("canonical/topic", qos=1, properties=properties)
    _sent_publish(engine)

    second = engine.queue_publish("", qos=1, properties=properties)
    assert engine.take_effects() == []
    stored = engine.store.get_out(second.mid or 0)
    assert stored is not None and stored.topic == "canonical/topic"

    from mqttium.packets import PubAckPacket

    feed_engine(engine, PubAckPacket(mid=first.mid or 0).encode(MQTTProtocolVersion.MQTTv5))
    assert _sent_publish(engine).topic == "canonical/topic"


def test_alias_state_resets_on_reconnect_even_when_session_is_present() -> None:
    engine = _connected_engine()
    # Keep resumable session state that is deliberately unrelated to the alias.
    engine.queue_publish("durable/unaliased", qos=1)
    engine.take_effects()
    engine.queue_publish("old/alias", qos=0, properties=_alias(1))
    _sent_publish(engine)

    engine.notify_transport_closed()
    engine.take_effects()
    engine.begin_connect()
    feed_engine(engine, _connack(session_present=True))
    replay = _sent_publish(engine)
    assert replay.topic == "durable/unaliased"

    with pytest.raises(ProtocolError, match="Unknown outbound topic alias"):
        engine.queue_publish("", qos=0, properties=_alias(1))


def test_alias_only_durable_replay_always_sends_full_canonical_topic() -> None:
    engine = _connected_engine()
    properties = _alias(1)
    engine.queue_publish("canonical/topic", qos=0, properties=properties)
    _sent_publish(engine)
    handle = engine.queue_publish("", b"x" * (64 * 1024), qos=1, properties=properties)
    assert _sent_publish(engine).topic == ""
    stored = engine.store.get_out(handle.mid or 0)
    assert stored is not None and stored.encoded_publish is None

    engine.notify_transport_closed()
    engine.take_effects()
    engine.begin_connect()
    feed_engine(engine, _connack(session_present=True))

    replay = _sent_publish(engine)
    assert replay.mid == handle.mid
    assert replay.topic == "canonical/topic"
    assert replay.dup is True


@pytest.mark.parametrize("store_kind", ["memory", "sqlite"])
def test_process_restart_replay_never_depends_on_old_alias(store_kind: str, tmp_path) -> None:
    store = (
        MemoryInflightStore()
        if store_kind == "memory"
        else SqliteInflightStore(tmp_path / "outbound-alias.db")
    )
    config = EngineConfig(
        client_id="alias-restart",
        protocol=MQTTProtocolVersion.MQTTv5,
        clean_start=False,
    )
    first = ProtocolEngine(config, store)
    first.begin_connect()
    feed_engine(first, _connack())
    first.take_effects()
    first.queue_publish("canonical/topic", qos=0, properties=_alias(1))
    _sent_publish(first)
    handle = first.queue_publish("", b"durable", qos=1, properties=_alias(1))
    assert _sent_publish(first).topic == ""
    first.notify_transport_closed()
    first.take_effects()

    second = ProtocolEngine(
        EngineConfig(
            client_id="alias-restart",
            protocol=MQTTProtocolVersion.MQTTv5,
            clean_start=False,
        ),
        store,
    )
    second.begin_connect()
    feed_engine(second, _connack(session_present=True))
    replay = _sent_publish(second)

    assert replay.mid == handle.mid
    assert replay.topic == "canonical/topic"
    if isinstance(store, SqliteInflightStore):
        store.close()


def test_failed_publish_batch_rolls_back_alias_mapping() -> None:
    engine = _connected_engine()
    requests = [
        ("transient/topic", b"one", 0, False, _alias(1)),
        ("invalid/#", b"two", 0, False, None),
    ]

    with pytest.raises(ProtocolError, match="wildcards"):
        engine.queue_publish_many(requests)

    assert engine.take_effects() == []
    with pytest.raises(ProtocolError, match="Unknown outbound topic alias"):
        engine.queue_publish("", qos=0, properties=_alias(1))
