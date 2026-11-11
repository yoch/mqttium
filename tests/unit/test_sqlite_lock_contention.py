"""What another connection holding the SQLite write lock does to a client.

A store file belongs to one client. When that rule is broken, the outcome
depends on whether anything had been changed when the lock was refused:

- the reader reserves the write lock before the engine applies a packet that
  will write, so a refusal there drops an unapplied packet: the connection is
  lost and retried like any other, and the broker resends what was never
  acknowledged;
- a ``publish()`` whose write is refused fails that call and leaves the client
  usable;
- a store failure that is not a lock refusal, or that happens after the engine
  changed state, still fail-stops the client.
"""

import asyncio
import sqlite3
from contextlib import closing

import pytest

import mqttium.persistence.sqlite as sqlite_store
from mqttium.api import AsyncClient
from mqttium.codec.buffer import RawPacket
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MQTTError
from mqttium.packets import PubAckPacket, PublishPacket
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.protocol.reconnect import ReconnectPolicy
from tests.support import ScriptedBrokerTransport, wait_until


@pytest.fixture
def short_busy_timeouts(monkeypatch):
    monkeypatch.setattr(sqlite_store, "_SQLITE_BUSY_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(sqlite_store, "_SQLITE_RESERVE_BUSY_TIMEOUT_SECONDS", 0.02)


class _SilentBroker(ScriptedBrokerTransport):
    """Completes CONNECT and never acknowledges a PUBLISH."""

    def handle_packet(self, raw):
        if raw.packet_type is PacketType.PUBLISH:
            return
        super().handle_packet(raw)


def _hold_write_lock(path) -> sqlite3.Connection:
    other = sqlite3.connect(path, timeout=0.02, isolation_level=None)
    other.execute("BEGIN IMMEDIATE")
    return other


def _inbound(qos: QoS, mid: int | None, protocol) -> bytes:
    return PublishPacket(
        topic="in", payload=b"x", qos=qos, retain=False, dup=False, mid=mid
    ).encode(protocol)


async def _client(path, *, broker=ScriptedBrokerTransport, on_message=None, **options):
    store = SqliteInflightStore(path)
    transports: list[ScriptedBrokerTransport] = []

    async def factory(host, port, *, ssl=None):
        transports.append(broker())
        return transports[-1]

    client = AsyncClient("locked", keepalive=0, clean_start=False, store=store, **options)
    client._transport_factory = factory
    causes: list[BaseException | None] = []

    async def on_disconnect(cause):
        causes.append(cause)

    client.on_disconnect = on_disconnect
    if on_message is not None:
        client.on_message = on_message
    await client.connect("unused")
    return client, store, transports, causes


def _rows(path, table: str) -> int:
    # sqlite3's own context manager ends a transaction; it does not close.
    with closing(sqlite3.connect(path)) as reader:
        return reader.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608


# ------------------------------------------------------------------ the store


def test_reserve_takes_the_write_lock_only_inside_a_batch(tmp_path, short_busy_timeouts):
    path = tmp_path / "inflight.db"
    store = SqliteInflightStore(path)
    store.reserve_batch_write()  # outside a batch: nothing to reserve
    other = _hold_write_lock(path)
    other.execute("ROLLBACK")
    with store.batch():
        store.reserve_batch_write()
        store.reserve_batch_write()  # the batch already owns its transaction
        with pytest.raises(sqlite3.OperationalError):
            other.execute("BEGIN IMMEDIATE")
    other.execute("BEGIN IMMEDIATE")  # released when the batch ended
    other.execute("ROLLBACK")
    other.close()
    store.close()


def test_refused_reservation_is_recognised_and_leaves_the_store_usable(
    tmp_path, short_busy_timeouts
):
    path = tmp_path / "inflight.db"
    store = SqliteInflightStore(path)
    other = _hold_write_lock(path)
    with store.batch():
        with pytest.raises(sqlite3.OperationalError) as refused:
            store.reserve_batch_write()
    assert store.is_lock_refusal(refused.value)
    assert not store.is_lock_refusal(sqlite3.OperationalError("disk I/O error"))
    assert not store.is_lock_refusal(RuntimeError("database is locked"))
    # The long timeout of irreversible writes is restored after the attempt.
    assert store._conn.execute("PRAGMA busy_timeout").fetchone()[0] == 50
    other.execute("ROLLBACK")
    other.close()
    with store.batch():
        store.reserve_batch_write()
    store.close()


def test_engine_names_the_packets_that_can_write_the_store():
    def raw(packet_type: PacketType, flags: int = 0) -> RawPacket:
        return RawPacket(packet_type, flags, b"")

    def engine(**options) -> ProtocolEngine:
        return ProtocolEngine(
            EngineConfig(client_id="c", protocol=MQTTProtocolVersion.MQTTv311, **options)
        )

    automatic = engine()
    for packet_type in (
        PacketType.CONNACK,
        PacketType.PUBACK,
        PacketType.PUBREC,
        PacketType.PUBREL,
        PacketType.PUBCOMP,
    ):
        assert automatic.may_write_store(raw(packet_type))
    for packet_type in (PacketType.PINGRESP, PacketType.SUBACK, PacketType.UNSUBACK):
        assert not automatic.may_write_store(raw(packet_type))
    assert not automatic.may_write_store(raw(PacketType.PUBLISH, 0b0000))  # QoS 0
    assert not automatic.may_write_store(raw(PacketType.PUBLISH, 0b0010))  # QoS 1, auto-ack
    assert automatic.may_write_store(raw(PacketType.PUBLISH, 0b0100))  # QoS 2
    # A manually acknowledged QoS 1 message is persisted until ack().
    assert engine(manual_ack=True).may_write_store(raw(PacketType.PUBLISH, 0b0010))


# ----------------------------------------------------------------- the client


async def test_locked_store_fails_the_publish_and_keeps_the_client(tmp_path, short_busy_timeouts):
    path = tmp_path / "inflight.db"
    client, store, _, _ = await _client(path)
    other = _hold_write_lock(path)
    try:
        with pytest.raises(sqlite3.OperationalError):
            await client.publish("t", b"x", qos=1)
        assert client.is_connected
        assert client.stats().receipts.publish == 0
    finally:
        other.execute("ROLLBACK")
        other.close()
    receipt = await client.publish("t", b"y", qos=1)
    await asyncio.wait_for(receipt.wait(), 2)
    await client.disconnect()
    store.close()


async def test_lock_refused_during_ingress_is_a_retryable_loss(tmp_path, short_busy_timeouts):
    path = tmp_path / "inflight.db"
    client, store, transports, causes = await _client(
        path, reconnect=ReconnectPolicy(initial_delay=0.01, max_delay=0.01)
    )
    protocol = transports[0].protocol
    other = _hold_write_lock(path)
    try:
        transports[0].push_rx(_inbound(QoS.EXACTLY_ONCE, 7, protocol))
        await wait_until(lambda: causes)
    finally:
        other.execute("ROLLBACK")
        other.close()

    assert isinstance(causes[0], sqlite3.OperationalError)
    assert client._local_terminal_failure is None
    # Nothing was applied for the dropped packet: no row, no reserved bytes,
    # no PUBREC for a message the store never recorded.
    assert _rows(path, "inbound") == 0
    assert client.stats().inbound.inflight == 0
    assert not any(raw.packet_type is PacketType.PUBREC for raw in _written(transports[0]))

    await wait_until(lambda: client.is_connected)
    # The broker resends what it never saw acknowledged; this time it lands.
    transports[-1].push_rx(_inbound(QoS.EXACTLY_ONCE, 7, protocol))
    await wait_until(lambda: _rows(path, "inbound") == 1)
    await wait_until(
        lambda: any(raw.packet_type is PacketType.PUBREC for raw in _written(transports[-1]))
    )
    await client.disconnect()
    store.close()


async def test_lock_refused_on_puback_leaves_the_receipt_pending(tmp_path, short_busy_timeouts):
    path = tmp_path / "inflight.db"
    client, store, transports, causes = await _client(
        path,
        broker=_SilentBroker,
        reconnect=ReconnectPolicy(initial_delay=0.01, max_delay=0.01),
    )
    receipt = await client.publish("t", b"x", qos=1)
    other = _hold_write_lock(path)
    try:
        transports[0].push_rx(PubAckPacket(mid=receipt.mid).encode(transports[0].protocol))
        await wait_until(lambda: causes)
        # The acknowledgement was never applied: the receipt is not settled
        # against a row that is still stored.
        assert not receipt.is_done()
        assert _rows(path, "outbound") == 1
        assert client.state is ConnectionState.RECONNECTING
        assert client._local_terminal_failure is None
    finally:
        other.execute("ROLLBACK")
        other.close()
    await client.disconnect()
    store.close()


async def test_lot_without_a_writing_packet_takes_no_lock(tmp_path, short_busy_timeouts):
    path = tmp_path / "inflight.db"
    received = []
    client, store, transports, causes = await _client(
        path, message_delivery="callback", on_message=received.append
    )
    other = _hold_write_lock(path)
    try:
        transports[0].push_rx(_inbound(QoS.AT_MOST_ONCE, None, transports[0].protocol))
        transports[0].push_rx(_inbound(QoS.AT_LEAST_ONCE, 3, transports[0].protocol))
        await wait_until(lambda: len(received) == 2)
        assert client.is_connected and not causes
    finally:
        other.execute("ROLLBACK")
        other.close()
    await client.disconnect()
    store.close()


async def test_store_failure_that_is_not_a_lock_refusal_still_fail_stops(
    tmp_path, short_busy_timeouts
):
    path = tmp_path / "inflight.db"
    client, store, transports, causes = await _client(
        path, reconnect=ReconnectPolicy(initial_delay=0.01, max_delay=0.01)
    )
    failure = sqlite3.OperationalError("disk I/O error")

    def broken_put_in(*args, **kwargs):
        raise failure

    store.put_in = broken_put_in
    transports[0].push_rx(_inbound(QoS.EXACTLY_ONCE, 7, transports[0].protocol))
    await wait_until(lambda: causes)
    await asyncio.sleep(0.05)
    assert causes[0] is failure
    assert client.state is ConnectionState.DISCONNECTED
    with pytest.raises(MQTTError, match="unusable"):
        await client.connect("unused")
    await client.disconnect()
    store.close()


def _written(transport: ScriptedBrokerTransport) -> list[RawPacket]:
    from mqttium.codec.buffer import IncrementalDecoder

    decoder = IncrementalDecoder()
    for chunk in transport.written:
        decoder.feed(chunk)
    return list(decoder.drain_packets())
