"""What happens today when another connection holds the SQLite write lock.

A store file belongs to one client. These tests pin the documented outcome of
breaking that rule, so that a later change making lock contention transient
alters it deliberately: an outbound write fails the call and leaves the client
usable; a write during ingress fail-stops the client.
"""

import asyncio
import sqlite3

import pytest

import mqttium.persistence.sqlite as sqlite_store
from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState, QoS
from mqttium.errors import MQTTError
from mqttium.packets import PublishPacket
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.reconnect import ReconnectPolicy
from tests.support import ScriptedBrokerTransport, transport_factory


@pytest.fixture
def short_busy_timeout(monkeypatch):
    monkeypatch.setattr(sqlite_store, "_SQLITE_BUSY_TIMEOUT_SECONDS", 0.05)


def _hold_write_lock(path) -> sqlite3.Connection:
    other = sqlite3.connect(path, timeout=0.05, isolation_level=None)
    other.execute("BEGIN IMMEDIATE")
    return other


async def _client(path, **options):
    store = SqliteInflightStore(path)
    transport = ScriptedBrokerTransport()
    client = AsyncClient("locked", keepalive=0, clean_start=False, store=store, **options)
    client._transport_factory = transport_factory(transport)
    await client.connect("unused")
    return client, store, transport


async def test_locked_store_fails_the_publish_and_keeps_the_client(tmp_path, short_busy_timeout):
    path = tmp_path / "inflight.db"
    client, store, _ = await _client(path)
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


async def test_locked_store_during_ingress_fail_stops_the_client(tmp_path, short_busy_timeout):
    path = tmp_path / "inflight.db"
    client, store, transport = await _client(
        path, reconnect=ReconnectPolicy(initial_delay=0.01, max_delay=0.01)
    )
    causes: list[BaseException | None] = []
    disconnected = asyncio.Event()

    async def on_disconnect(cause):
        causes.append(cause)
        disconnected.set()

    client.on_disconnect = on_disconnect
    other = _hold_write_lock(path)
    try:
        transport.push_rx(
            PublishPacket(
                topic="in", payload=b"x", qos=QoS.EXACTLY_ONCE, retain=False, dup=False, mid=7
            ).encode(transport.protocol)
        )
        await asyncio.wait_for(disconnected.wait(), 5)
    finally:
        other.execute("ROLLBACK")
        other.close()
    assert isinstance(causes[0], sqlite3.OperationalError)
    await asyncio.sleep(0.1)
    # Fail-stop: the reconnect policy does not apply, and the client is done.
    assert client.state is ConnectionState.DISCONNECTED
    with pytest.raises(MQTTError, match="unusable"):
        await client.connect("unused")
    await client.disconnect()
    store.close()
