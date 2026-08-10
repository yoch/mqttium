"""Non-replayable requests fail immediately when a transport is lost."""

from __future__ import annotations

import asyncio
import gc

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

    receipt = PublishReceipt(
        mid=13,
        qos=QoS.AT_LEAST_ONCE,
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
    assert not receipt.is_done()

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
    receipt = PublishReceipt(mid=3, qos=QoS.AT_LEAST_ONCE)
    client._register_publish_receipt(3, receipt)
    error = MQTTError("terminal")

    client._fail_pending(error)

    with pytest.raises(MQTTError, match="terminal"):
        await sub
    with pytest.raises(MQTTError, match="terminal"):
        await unsub
    assert receipt.is_done()
    with pytest.raises(MQTTError, match="terminal"):
        await receipt.wait()
    assert client._receipts == {}


async def test_failed_receipt_that_is_never_awaited_stays_silent() -> None:
    """The library installs no logging, so a stray future would print."""
    loop = asyncio.get_running_loop()
    handled: list[dict[str, object]] = []
    loop.set_exception_handler(lambda _loop, context: handled.append(context))

    client = AsyncClient(client_id="silent-receipt")
    receipt = PublishReceipt(mid=41, qos=QoS.AT_LEAST_ONCE)
    client._register_publish_receipt(41, receipt)

    client._fail_pending(MQTTError("terminal"))
    assert receipt.is_done()

    del receipt
    gc.collect()
    await asyncio.sleep(0)

    assert handled == []


async def test_receipt_never_awaited_allocates_no_completion_primitive() -> None:
    """publish_nowait's whole point: nobody waits, so nothing is built."""
    client = AsyncClient(client_id="lazy-receipt", max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED

    receipt = client.publish_nowait("lazy/qos1", b"x", qos=1)

    assert receipt._future is None
    client._settle_publish(receipt.mid, None)
    assert receipt.is_done()
    assert receipt._future is None
    await receipt.wait()


async def test_receipt_awaited_before_completion_resolves_once() -> None:
    client = AsyncClient(client_id="await-receipt", max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    receipt = client.publish_nowait("lazy/qos1", b"x", qos=1)

    waiter = asyncio.ensure_future(receipt.wait())
    await asyncio.sleep(0)
    assert receipt._future is not None

    client._settle_publish(receipt.mid, None)
    await waiter
    assert receipt.is_done()
    # A second settle must not raise InvalidStateError on the resolved future.
    receipt._settle()
