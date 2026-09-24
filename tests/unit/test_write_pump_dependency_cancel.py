"""Transport-originated CancelledError belongs to the failed writer, not lifecycle cancellation."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.api._writer import WritePump
from mqttium.enums import PacketType, QoS
from tests.support import ScriptedBrokerTransport, transport_factory, wait_until


class _CancelledPublishTransport(ScriptedBrokerTransport):
    def __init__(self) -> None:
        super().__init__()
        self.failure = asyncio.CancelledError("transport self-cancel")
        self.publish_attempts = 0

    async def write(self, data: bytes) -> None:
        if data and data[0] & 0xF0 == int(PacketType.PUBLISH):
            self.publish_attempts += 1
            raise self.failure
        await super().write(data)

    async def write_many(self, parts: list[bytes]) -> None:
        await self.write(b"".join(parts))


async def test_transport_cancelled_error_retires_writer_generation_and_receipt() -> None:
    transport = _CancelledPublishTransport()
    client = AsyncClient("writer-dependency-cancel")
    client._transport_factory = transport_factory(transport)
    await client.connect("fake")
    try:
        receipt = await client.publish("writer/cancel", b"x", qos=QoS.AT_LEAST_ONCE)

        await wait_until(lambda: not client.is_connected)

        assert transport.publish_attempts == 1
        assert client._disconnect_exc is transport.failure
        assert receipt.is_done()
        assert not client._receipts
        assert client._write_pump.epoch == client._connection_epoch
        with pytest.raises(asyncio.CancelledError) as caught:
            await receipt.wait()
        assert caught.value is transport.failure
    finally:
        await client.disconnect()


class _BlockingTransport:
    def __init__(self) -> None:
        self.entered = asyncio.Event()

    async def write(self, data: bytes) -> None:
        del data
        self.entered.set()
        await asyncio.Event().wait()

    async def read(self, n: int = 65536) -> bytes:
        del n
        await asyncio.Event().wait()
        return b""

    async def close(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False


async def test_real_writer_task_cancellation_remains_lifecycle_owned() -> None:
    failures: list[BaseException] = []

    async def on_failure(exc: BaseException) -> None:
        failures.append(exc)

    pump = WritePump(max_bytes=1024, max_messages=2, on_failure=on_failure)
    transport = _BlockingTransport()
    pump.start(transport)
    pump._eager_armed = False
    assert pump.try_enqueue(b"x")
    await asyncio.wait_for(transport.entered.wait(), timeout=1)

    await pump.stop()

    assert failures == []
    assert pump.task is None
