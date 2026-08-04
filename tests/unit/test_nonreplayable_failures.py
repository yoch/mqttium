"""Non-replayable requests fail immediately when a transport is lost."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.api.models import PublishReceipt
from mqttium.enums import ConnectionState, QoS
from mqttium.errors import MQTTError
from mqttium.protocol.reconnect import ReconnectPolicy


class _ClosedTransport:
    def __init__(self) -> None:
        self.closed = False

    async def read(self, n: int = 65536) -> bytes:
        return b""

    async def write(self, data: bytes) -> None:
        return None

    async def close(self) -> None:
        self.closed = True

    def is_closing(self) -> bool:
        return self.closed


async def test_transport_loss_fails_subscriptions_but_preserves_publish_receipt() -> None:
    reconnect = ReconnectPolicy(
        enabled=True,
        initial_delay=60.0,
        max_delay=60.0,
        stable_after=60.0,
    )
    client = AsyncClient(client_id="nonreplayable", reconnect=reconnect)
    client._host = "fake"
    client._port = 1883
    client._engine.state = ConnectionState.CONNECTED
    client._transport = _ClosedTransport()

    loop = asyncio.get_running_loop()
    sub = loop.create_future()
    unsub = loop.create_future()
    client._sub_futs[11] = sub
    client._unsub_futs[12] = unsub

    publish_event = asyncio.Event()
    receipt = PublishReceipt(
        mid=13,
        qos=QoS.AT_LEAST_ONCE,
        _event=publish_event,
    )
    client._register_publish_receipt(13, receipt)

    await client._read_loop()

    with pytest.raises(MQTTError, match="Connection closed"):
        await sub
    with pytest.raises(MQTTError, match="Connection closed"):
        await unsub
    assert client._sub_futs == {}
    assert client._unsub_futs == {}
    assert client._receipts[13] is receipt
    assert not publish_event.is_set()

    if client._reconnect_task is not None:
        client._reconnect_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await client._reconnect_task
        client._reconnect_task = None


async def test_fail_pending_still_fails_all_operation_types() -> None:
    client = AsyncClient(client_id="fail-all")
    loop = asyncio.get_running_loop()
    sub = loop.create_future()
    unsub = loop.create_future()
    client._sub_futs[1] = sub
    client._unsub_futs[2] = unsub
    event = asyncio.Event()
    client._register_publish_receipt(
        3,
        PublishReceipt(
            mid=3,
            qos=QoS.AT_LEAST_ONCE,
            _event=event,
        ),
    )
    error = MQTTError("terminal")

    client._fail_pending(error)

    with pytest.raises(MQTTError, match="terminal"):
        await sub
    with pytest.raises(MQTTError, match="terminal"):
        await unsub
    assert event.is_set()
    assert client._receipts == {}
