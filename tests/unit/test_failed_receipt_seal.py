"""A client never sends a publication whose receipt it already failed (#521).

When a receipt fails terminally (refused CONNACK, final transport loss,
disconnect()), the same AsyncClient must not later publish or replay it
silently. The durable row stays for another client or process to recover,
and its packet identifier stays reserved in this client.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from mqttium.api import AsyncClient, PublishMessage
from mqttium.codec.buffer import RawPacket
from mqttium.enums import MQTTProtocolVersion, OutboundQoSState, PacketType
from mqttium.errors import MQTTError
from mqttium.packets import PubAckPacket, PublishPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from tests.support import ScriptedBrokerTransport, transport_factory, wait_until

V311 = MQTTProtocolVersion.MQTTv311


class _Broker(ScriptedBrokerTransport):
    """Answers CONNECT with a scripted CONNACK and never acknowledges PUBLISH."""

    def __init__(self, *, session_present: bool = False, return_code: int = 0) -> None:
        super().__init__()
        self.session_present = session_present
        self.return_code = return_code

    def handle_packet(self, raw: RawPacket) -> None:
        if raw.packet_type is PacketType.CONNECT:
            flags = 1 if self.session_present and self.return_code == 0 else 0
            self.push_rx(encode_frame(PacketType.CONNACK, 0, bytes((flags, self.return_code))))
        elif raw.packet_type is PacketType.PUBLISH:
            self.publishes.append(PublishPacket.decode(raw.flags, raw.remaining, self.protocol))
        else:
            super().handle_packet(raw)


def _store(kind: str, path: Path) -> MemoryInflightStore | SqliteInflightStore:
    return MemoryInflightStore() if kind == "memory" else SqliteInflightStore(path)


def _client(store, broker: _Broker) -> AsyncClient:
    client = AsyncClient("sealed", clean_start=False, store=store, keepalive=0)
    client._transport_factory = transport_factory(broker)
    return client


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
async def test_disconnect_failed_receipt_is_not_replayed_by_the_same_client(
    kind: str, tmp_path: Path
) -> None:
    store = _store(kind, tmp_path / "s.db")
    first = _Broker()
    client = _client(store, first)
    await client.connect("fake")
    receipt = await client.publish("sealed/t", b"x", qos=1)
    mid = receipt.mid
    await wait_until(lambda: len(first.publishes) == 1)
    await client.disconnect()
    assert isinstance(receipt._error, MQTTError)

    # The same client resumes the session: nothing is replayed.
    second = _Broker(session_present=True)
    client._transport_factory = transport_factory(second)
    await client.connect("fake")
    try:
        fresh = await client.publish("fresh/t", b"y", qos=1)
        await wait_until(lambda: len(second.publishes) == 1)
        assert [p.topic for p in second.publishes] == ["fresh/t"]
        assert fresh.mid != mid  # the sealed identifier stays reserved
        record = store.get_out(mid)
        assert record is not None and record.state is OutboundQoSState.WAIT_PUBACK
    finally:
        await client.disconnect()

    # Another client (a restart) still recovers the row from the store.
    third = _Broker(session_present=True)
    recovered = _client(store, third)
    await recovered.connect("fake")
    try:
        await wait_until(lambda: any(p.topic == "sealed/t" for p in third.publishes))
        replayed = next(p for p in third.publishes if p.topic == "sealed/t")
        assert replayed.dup and replayed.mid == mid
    finally:
        await recovered.disconnect()
        if isinstance(store, SqliteInflightStore):
            store.close()


async def test_offline_publication_failed_by_a_refused_connack_is_never_sent() -> None:
    store = MemoryInflightStore()
    refused = _Broker(return_code=5)
    client = _client(store, refused)
    receipt = await client.publish("offline/t", b"x", qos=1)
    with pytest.raises(MQTTError):
        await client.connect("fake")
    assert receipt.is_done() and receipt._error is not None

    accepted = _Broker()
    client._transport_factory = transport_factory(accepted)
    await client.connect("fake")
    try:
        await client.publish("after/t", b"y", qos=1)
        await wait_until(lambda: len(accepted.publishes) == 1)
        assert [p.topic for p in accepted.publishes] == ["after/t"]
        assert store.get_out(receipt.mid) is not None  # kept for recovery
    finally:
        await client.disconnect()


async def test_broker_completion_frees_a_sealed_identifier() -> None:
    store = MemoryInflightStore()
    first = _Broker()
    client = _client(store, first)
    await client.connect("fake")
    receipt = await client.publish("sealed/t", b"x", qos=1)
    mid = receipt.mid
    assert mid is not None
    await wait_until(lambda: len(first.publishes) == 1)
    await client.disconnect()

    second = _Broker(session_present=True)
    client._transport_factory = transport_factory(second)
    await client.connect("fake")
    try:
        assert client._engine.packet_ids.in_use(mid)
        second.push_rx(PubAckPacket(mid=mid).encode(V311))  # broker had it
        await wait_until(lambda: store.get_out(mid) is None)
        assert not client._engine.packet_ids.in_use(mid)
        assert client.stats().outbound.unacknowledged_messages == 0
    finally:
        await client.disconnect()


async def test_publish_many_failed_entries_are_sealed_too() -> None:
    store = MemoryInflightStore()
    first = _Broker()
    client = _client(store, first)
    await client.connect("fake")
    batch = await client.publish_many(
        [PublishMessage("batch/a", b"a", qos=1), PublishMessage("batch/b", b"b", qos=1)]
    )
    await wait_until(lambda: len(first.publishes) == 2)
    await client.disconnect()
    with pytest.raises(MQTTError):
        await batch.wait()

    second = _Broker(session_present=True)
    client._transport_factory = transport_factory(second)
    await client.connect("fake")
    try:
        await client.publish("after/t", b"y", qos=1)
        await wait_until(lambda: len(second.publishes) == 1)
        assert [p.topic for p in second.publishes] == ["after/t"]
    finally:
        await client.disconnect()


def test_clean_session_drops_sealed_sent_rows_and_keeps_sealed_queued_ones() -> None:
    from mqttium.enums import QoS
    from mqttium.protocol.engine import EngineConfig, ProtocolEngine
    from mqttium.types import OutboundMessage
    from tests.support import feed_engine, stored_record

    store = MemoryInflightStore()
    for mid, state in ((1, OutboundQoSState.WAIT_PUBACK), (2, OutboundQoSState.QUEUED)):
        store.put_out(
            stored_record(
                OutboundMessage(
                    mid=mid,
                    topic=f"t/{mid}",
                    payload=b"p",
                    qos=QoS.AT_LEAST_ONCE,
                    retain=False,
                    state=state,
                )
            )
        )
    engine = ProtocolEngine(EngineConfig(client_id="c", clean_start=False), store)
    engine.seal_publications([1, 2])
    engine.seal_publications([1])  # idempotent
    assert engine.outbound.unacknowledged_messages == 0

    engine.begin_connect()
    engine.take_effects()
    feed_engine(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))  # no session
    sends = [e for e in engine.take_effects() if e.kind.name == "SEND"]
    assert sends == []  # the sealed QUEUED row is not launched either
    assert store.get_out(1) is None and not engine.packet_ids.in_use(1)
    assert store.get_out(2) is not None and engine.packet_ids.in_use(2)


async def test_a_publication_that_cannot_be_sealed_retires_the_client() -> None:
    class _Unsealable(MemoryInflightStore):
        def out_meta(self, mid: int):  # type: ignore[no-untyped-def]
            raise OSError("store unavailable")

    store = _Unsealable()
    broker = _Broker()
    client = _client(store, broker)
    await client.connect("fake")
    await client.publish("t", b"x", qos=1)
    await wait_until(lambda: len(broker.publishes) == 1)
    await client.disconnect()
    assert isinstance(client._local_terminal_failure, OSError)
    with pytest.raises(MQTTError, match="unusable"):
        await client.connect("fake")
