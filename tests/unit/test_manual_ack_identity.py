"""A delivered handle cannot acknowledge a later exchange reusing its MID."""

from __future__ import annotations

import asyncio
from contextlib import closing
from dataclasses import replace

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import InboundQoSState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import ProtocolError
from mqttium.packets import PublishPacket, PubRelPacket, encode_frame
from mqttium.persistence import MemoryInflightStore, SqliteInflightStore
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.types import Message, Properties
from tests.support import ScriptedBrokerTransport, feed_engine, wait_until

PROTOCOLS = (MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5)


def _connack(protocol, *, present=False):
    return encode_frame(
        PacketType.CONNACK,
        0,
        bytes((int(present), 0)) + (b"\x00" if protocol is MQTTProtocolVersion.MQTTv5 else b""),
    )


def _engine(protocol=MQTTProtocolVersion.MQTTv311, *, store=None, present=False, manual=True):
    engine = ProtocolEngine(
        EngineConfig(
            client_id="ack-identity",
            protocol=protocol,
            clean_start=False,
            manual_ack=manual,
            connect_properties=Properties({"session_expiry_interval": 60})
            if protocol is MQTTProtocolVersion.MQTTv5
            else None,
        ),
        store,
    )
    engine.begin_connect()
    feed_engine(engine, _connack(protocol, present=present))
    engine.take_effects()
    return engine


def _wire(protocol, qos, *, mid=9, payload=b"value", dup=False):
    return PublishPacket(
        topic="ack/identity", payload=payload, qos=QoS(qos), retain=False, dup=dup, mid=mid
    ).encode(protocol)


def _deliver(engine, qos, *, mid=9, dup=False):
    feed_engine(engine, _wire(engine.config.protocol, qos, mid=mid, dup=dup))
    return next(
        effect.data
        for effect in engine.take_effects()
        if effect.kind in (EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE)
    )


def _resume(engine, *, present=True):
    engine.notify_transport_closed()
    engine.take_effects()
    engine.begin_connect()
    feed_engine(engine, _connack(engine.config.protocol, present=present))
    return engine.take_effects()


@pytest.mark.parametrize("protocol", PROTOCOLS)
@pytest.mark.parametrize("qos", (1, 2))
@pytest.mark.parametrize("reconnect", (False, True))
async def test_stale_public_handle_does_not_ack_reused_mid(protocol, qos, reconnect):
    client = AsyncClient(
        "ack-identity", protocol=protocol, manual_ack=True, reconnect=ReconnectPolicy(enabled=False)
    )
    brokers = []

    async def factory(*args, **kwargs):
        broker = ScriptedBrokerTransport(protocol=protocol)
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    await client.connect("fake")
    try:
        stream = client.messages()
        brokers[-1].push_rx(_wire(protocol, qos, payload=b"old"))
        old = await asyncio.wait_for(anext(stream), 1)
        if qos == 2:
            brokers[-1].push_rx(PubRelPacket(mid=9).encode(protocol))
            await wait_until(
                lambda: client._engine.store.in_meta(9).state is InboundQoSState.WAIT_USER_ACK
            )
        if reconnect:
            await client.disconnect()
            await client.connect("fake")
            stream = client.messages()
        else:
            await client.ack(old)
        brokers[-1].push_rx(_wire(protocol, qos, payload=b"new"))
        new = await asyncio.wait_for(anext(stream), 1)
        if qos == 2:
            brokers[-1].push_rx(PubRelPacket(mid=9).encode(protocol))
            await wait_until(
                lambda: client._engine.store.in_meta(9).state is InboundQoSState.WAIT_USER_ACK
            )
        await client._write_pump.join()
        before = tuple(brokers[-1].written)
        record = client._engine.store.get_in(9)
        stats = client._engine.inbound.stats()
        with pytest.raises(ProtocolError, match="active inbound acknowledgement"):
            await client.ack(old)
        assert client._engine.store.get_in(9) == record
        assert client._engine.inbound.stats() == stats
        assert tuple(brokers[-1].written) == before
        await client.ack(new)
        assert client._engine.store.in_count() == 0
        assert client._engine.inbound._ack_tokens == {}
    finally:
        await client.disconnect()


@pytest.mark.parametrize("qos", (1, 2))
def test_foreign_and_reconstructed_handles_are_rejected(qos):
    engine = _engine()
    original = _deliver(engine, qos)
    foreign = _deliver(_engine(), qos)
    for invalid in (
        foreign,
        replace(original),
        Message(original.topic, original.payload, qos=original.qos, mid=original.mid),
    ):
        assert invalid == original  # Equality is not acknowledgement ownership.
        with pytest.raises(ProtocolError, match="active inbound acknowledgement"):
            engine.ack(9, message=invalid)
        assert engine.store.in_count() == 1
    assert "_ack_token" not in repr(original)
    engine.ack(9, message=original)


@pytest.mark.parametrize("protocol", PROTOCOLS)
@pytest.mark.parametrize("qos", (1, 2))
def test_handle_survives_resumed_session_but_not_replacement(protocol, qos):
    engine = _engine(protocol)
    original = _deliver(engine, qos)
    engine.mark_inbound_delivered(9)
    if qos == 2:
        feed_engine(engine, PubRelPacket(mid=9).encode(protocol))
        engine.take_effects()
    effects = _resume(engine)
    for effect in effects:
        if isinstance(effect.data, Message):
            assert effect.data._ack_token is original._ack_token
    engine.ack(9, message=original)
    assert engine.inbound._ack_tokens == {}
    with pytest.raises(ProtocolError):
        engine.ack(9, message=original)
    another = _deliver(engine, qos)
    _resume(engine, present=False)
    replacement = _deliver(engine, qos)
    assert replacement._ack_token is not another._ack_token
    with pytest.raises(ProtocolError):
        engine.ack(9, message=another)


def test_qos1_duplicate_and_ordered_ack_intent_keep_exchange_identity():
    engine = _engine()
    first = _deliver(engine, 1, mid=1)
    second = _deliver(engine, 1, mid=2)
    duplicate = _deliver(engine, 1, mid=2, dup=True)
    assert duplicate._ack_token is second._ack_token
    engine.ack(2, message=second)
    engine.ack(2, message=duplicate)
    assert engine.store.in_count() == 2
    _resume(engine)
    engine.ack(2, message=second)
    engine.ack(1, message=first)
    assert engine.store.in_count() == 0
    assert engine.inbound._ack_tokens == {}


@pytest.mark.parametrize("protocol", PROTOCOLS)
def test_qos2_ack_before_pubrel_keeps_token_until_terminal_completion(protocol):
    engine = _engine(protocol)
    message = _deliver(engine, 2)
    engine.ack(9, message=message)
    engine.ack(9, message=message)
    assert engine.inbound._ack_tokens[9] is message._ack_token
    _resume(engine)
    engine.ack(9, message=message)
    feed_engine(engine, PubRelPacket(mid=9).encode(protocol))
    assert engine.inbound._ack_tokens == {}
    with pytest.raises(ProtocolError):
        engine.ack(9, message=message)


@pytest.mark.parametrize("qos", (1, 2))
def test_sqlite_recovery_issues_new_client_identity(tmp_path, qos):
    path = tmp_path / "identity.sqlite"
    with closing(SqliteInflightStore(path)) as store:
        original_engine = _engine(store=store)
        original = _deliver(original_engine, qos)
        original_engine.mark_inbound_delivered(9)
        if qos == 2:
            feed_engine(original_engine, PubRelPacket(mid=9).encode())
    with closing(SqliteInflightStore(path)) as store:
        engine = ProtocolEngine(
            EngineConfig(client_id="ack-identity", clean_start=False, manual_ack=True), store
        )
        effects = _resume(engine)
        recovered = next(effect.data for effect in effects if isinstance(effect.data, Message))
        assert recovered._ack_token is not original._ack_token
        with pytest.raises(ProtocolError):
            engine.ack(9, message=original)
        engine.ack(9, message=recovered)
        assert store.in_count() == 0
        assert engine.inbound._ack_tokens == {}


@pytest.mark.parametrize("failure", ("raise", "refuse"))
def test_store_completion_failure_preserves_token(monkeypatch, failure):
    engine = _engine()
    message = _deliver(engine, 1)
    original = MemoryInflightStore.complete_in

    def fail(*args):
        if failure == "raise":
            raise OSError("store fault")
        return None

    monkeypatch.setattr(MemoryInflightStore, "complete_in", fail)
    with pytest.raises((OSError, RuntimeError)):
        engine.ack(9, message=message)
    assert engine.store.in_count() == 1
    assert engine.inbound._ack_tokens[9] is message._ack_token
    monkeypatch.setattr(MemoryInflightStore, "complete_in", original)
    engine.ack(9, message=message)
    assert engine.inbound._ack_tokens == {}


def test_auto_ack_does_not_allocate_identity_index():
    engine = _engine(manual=False)
    message = _deliver(engine, 2)
    assert engine.inbound._ack_tokens is None
    assert message._ack_token is None


@pytest.mark.parametrize("protocol", PROTOCOLS)
@pytest.mark.parametrize("qos", (1, 2))
@pytest.mark.parametrize("automatic", (False, True))
async def test_public_handle_survives_actual_session_resume(protocol, qos, automatic):
    class SessionBroker(ScriptedBrokerTransport):
        def __init__(self, *, present):
            super().__init__(protocol=protocol)
            self.present = present

        def handle_packet(self, raw):
            if raw.packet_type is PacketType.CONNECT:
                self.push_rx(_connack(protocol, present=self.present))
            else:
                super().handle_packet(raw)

    client = AsyncClient(
        "ack-resume",
        protocol=protocol,
        manual_ack=True,
        clean_start=False,
        connect_properties=Properties({"session_expiry_interval": 60})
        if protocol is MQTTProtocolVersion.MQTTv5
        else None,
        reconnect=ReconnectPolicy(enabled=automatic, initial_delay=0, max_delay=0, stable_after=0),
    )
    brokers = []

    async def factory(*args, **kwargs):
        broker = SessionBroker(present=bool(brokers))
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    await client.connect("fake")
    try:
        stream = client.messages()
        brokers[0].push_rx(_wire(protocol, qos))
        message = await asyncio.wait_for(anext(stream), 1)
        if qos == 2:
            brokers[0].push_rx(PubRelPacket(mid=9).encode(protocol))
            await wait_until(
                lambda: client._engine.store.in_meta(9).state is InboundQoSState.WAIT_USER_ACK
            )
        if automatic:
            brokers[0].push_rx(b"")
            await wait_until(lambda: len(brokers) == 2 and client.is_connected)
        else:
            await client.disconnect()
            await client.connect("fake")
        await client.ack(message)
        assert client._engine.store.in_count() == 0
        assert client._engine.inbound._ack_tokens == {}
    finally:
        await client.disconnect()
