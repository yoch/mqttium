"""Reconnect-loop retry and terminal-refusal regressions."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.api.models import PublishReceipt
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import ProtocolError
from mqttium.packets import encode_frame
from mqttium.protocol.reconnect import ReconnectPolicy


class _ConnAckTransport:
    def __init__(self, reason_code: int = 0) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._reason_code = reason_code
        self._closed = False
        self._sent_connack = False

    async def write(self, data: bytes) -> None:
        if not self._sent_connack:
            self._sent_connack = True
            self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, bytes((0, self._reason_code))))

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closed


def _policy(*, max_retries: int | None = None) -> ReconnectPolicy:
    return ReconnectPolicy(
        enabled=True,
        initial_delay=0.0,
        max_delay=0.0,
        max_retries=max_retries,
        stable_after=0.0,
        connect_timeout=0.1,
    )


async def test_reconnect_retries_transport_failures_until_success() -> None:
    policy = _policy()
    client = AsyncClient(reconnect=policy)
    client._host = "fake"
    client._port = 1883
    calls = 0

    async def factory(host: str, port: int, *, ssl: object = None):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ConnectionRefusedError("broker unavailable")
        return _ConnAckTransport()

    client._transport_factory = factory
    client._intentional_disconnect = False

    await asyncio.wait_for(client._reconnect_loop(), timeout=1.0)

    assert calls == 3
    assert client.is_connected
    assert policy.attempt == 0  # stable success resets backoff state
    await client.disconnect()


async def test_reconnect_exhaustion_fails_pending_receipts() -> None:
    policy = _policy(max_retries=2)
    client = AsyncClient(reconnect=policy)
    client._host = "fake"
    client._port = 1883
    receipt = PublishReceipt(mid=7, qos=QoS.AT_LEAST_ONCE)
    client._receipts[7] = receipt
    calls = 0

    async def factory(host: str, port: int, *, ssl: object = None):
        nonlocal calls
        calls += 1
        raise ConnectionRefusedError("broker unavailable")

    client._transport_factory = factory
    client._intentional_disconnect = False

    await asyncio.wait_for(client._reconnect_loop(), timeout=1.0)

    assert calls == 2
    assert receipt.is_done()
    with pytest.raises(ConnectionRefusedError, match="broker unavailable"):
        await receipt.wait()


async def test_terminal_connack_reason_stops_reconnect_without_string_parsing() -> None:
    policy = _policy()
    client = AsyncClient(protocol=MQTTProtocolVersion.MQTTv311, reconnect=policy)
    client._host = "fake"
    client._port = 1883
    calls = 0

    async def factory(host: str, port: int, *, ssl: object = None):
        nonlocal calls
        calls += 1
        return _ConnAckTransport(reason_code=5)

    client._transport_factory = factory
    client._intentional_disconnect = False
    receipt = PublishReceipt(mid=7, qos=QoS.AT_LEAST_ONCE)
    client._receipts[7] = receipt

    await asyncio.wait_for(client._reconnect_loop(), timeout=1.0)

    assert calls == 1
    assert client._last_connack_reason == 5
    assert isinstance(client._disconnect_exc, ProtocolError)
    assert not client._will_reconnect()

    # Terminal refusal must also be visible to producers already parked on
    # outbound admission capacity. Once final teardown wakes them, they must
    # fail instead of treating a stopped reconnect loop as still live.
    client._teardown_final = True
    assert client._publish_wait_failure() is client._disconnect_exc
    with pytest.raises(ProtocolError, match="Connection refused: reason_code=5"):
        await receipt.wait()
