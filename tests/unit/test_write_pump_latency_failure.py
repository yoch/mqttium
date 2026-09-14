"""Latency shortcuts preserve writer ownership and report ambiguous failure."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient, PublishMessage
from mqttium.api._effects import StaleConnectionEffect
from mqttium.api._writer import WritePump
from mqttium.errors import PublishBatchError
from tests.support import ScriptedBrokerTransport, transport_factory, wait_until


class _RecordingTransport:
    def __init__(self, failure: BaseException | None = None) -> None:
        self.failure = failure
        self.eager_calls: list[bytes] = []
        self.awaited_calls: list[bytes] = []

    def write_nowait(self, data: bytes) -> bool:
        self.eager_calls.append(data)
        if self.failure is not None:
            raise self.failure
        return False

    async def write(self, data: bytes) -> None:
        self.awaited_calls.append(data)

    async def write_many(self, parts: list[bytes]) -> None:
        self.awaited_calls.extend(parts)

    async def close(self) -> None:
        pass

    def is_closing(self) -> bool:
        return False


@pytest.mark.parametrize("segmented", [False, True])
async def test_restored_latency_work_does_not_release_a_waiting_join(segmented):
    failures = []

    async def on_failure(exc):
        failures.append(exc)

    pump = WritePump(max_bytes=1 << 20, max_messages=32, on_failure=on_failure)
    transport = _RecordingTransport()
    pump._write_nowait = transport.write_nowait
    items = [b"x" * 4096] * 12
    if segmented:
        items[-1] = (b"h", b"p" * 4095)
    for item in items:
        assert pump.try_enqueue(item)
    joined = asyncio.create_task(pump.join())
    try:
        await asyncio.sleep(0)
        assert not joined.done()
        assert pump._try_flush_latency_batch() is False
        await asyncio.sleep(0)
        assert not joined.done()
        assert pump.queued_messages == pump.resident_messages == 12
        assert pump.queued_bytes == 12 * 4096
        pump.start(transport)
        await asyncio.wait_for(joined, 1)
        expected = [
            part for item in items for part in (item if isinstance(item, tuple) else (item,))
        ]
        assert transport.awaited_calls == expected
        assert not failures
        assert pump.queued_bytes == pump.resident_messages == 0
    finally:
        joined.cancel()
        await pump.stop()
        pump.discard()


@pytest.mark.parametrize("writer_started", [False, True])
@pytest.mark.parametrize("failure_type", [OSError, asyncio.CancelledError])
async def test_ambiguous_latency_failure_is_retired_by_existing_writer(
    writer_started, failure_type
):
    failure = failure_type("ambiguous latency write")
    transport = _RecordingTransport(failure)
    failures = []
    reported = asyncio.Event()

    async def on_failure(exc):
        failures.append(exc)
        reported.set()

    pump = WritePump(max_bytes=1 << 20, max_messages=16, on_failure=on_failure)
    pump.start(transport)
    if writer_started:
        await asyncio.sleep(0)
    pump._eager_armed = False
    parts = [bytes([index]) * 64 for index in range(16)]
    initial_epoch = pump.epoch
    for part in parts:
        assert pump.try_enqueue(part, epoch=initial_epoch)
    # This producer already belongs to the failed generation, even though its
    # task has not run yet. It must fail instead of reusing released credits.
    late = asyncio.create_task(pump.enqueue(b"late", epoch=initial_epoch))
    try:
        with pytest.raises(failure_type) as caught:
            pump._try_flush_latency_batch()
        assert caught.value is failure
        assert pump.epoch == initial_epoch + 1
        assert pump._write_nowait is None
        assert pump.queued_messages == pump.resident_messages == 16
        assert pump.queued_bytes == 1024
        # Omitting an epoch must not opt into the freshly invalidated writer.
        with pytest.raises(StaleConnectionEffect):
            pump.try_enqueue(b"implicit data")
        with pytest.raises(StaleConnectionEffect):
            pump.try_enqueue_ack(b"implicit ack")
        with pytest.raises(StaleConnectionEffect):
            await pump.enqueue(b"implicit async data")
        with pytest.raises(StaleConnectionEffect):
            await pump.enqueue_ack(b"implicit async ack")
        with pytest.raises(StaleConnectionEffect):
            await asyncio.wait_for(late, 1)
        await asyncio.wait_for(reported.wait(), 1)
        await asyncio.wait_for(pump.join(), 1)
        assert failures == [failure]
        assert transport.eager_calls == [b"".join(parts)]
        assert not transport.awaited_calls
        assert pump.queued_messages == pump.resident_messages == pump.queued_bytes == 0
        assert pump.epoch == initial_epoch + 1
        assert pump.task is not None and pump.task.done()
        with pytest.raises(StaleConnectionEffect):
            await asyncio.wait_for(pump.enqueue(b"after stopped writer"), 1)
        with pytest.raises(StaleConnectionEffect):
            await pump._enqueue_after_wait(b"after capacity wait", 19, epoch=pump.epoch)
    finally:
        late.cancel()
        await pump.stop()
        pump.discard()


async def test_cancel_before_failure_dispatch_keeps_restored_ownership_until_discard():
    failures = []

    async def on_failure(exc):
        failures.append(exc)

    failure = OSError("latency write failed")
    transport = _RecordingTransport(failure)
    pump = WritePump(max_bytes=1 << 20, max_messages=16, on_failure=on_failure)
    pump.start(transport)
    pump._eager_armed = False
    for _ in range(16):
        assert pump.try_enqueue(b"frame")
    with pytest.raises(OSError):
        pump._try_flush_latency_batch()
    await pump.stop()
    assert not failures
    assert pump.queued_messages == pump.resident_messages == 16
    assert pump.queued_bytes == 80
    pump.discard()
    await asyncio.wait_for(pump.join(), 1)
    assert pump.queued_messages == pump.resident_messages == pump.queued_bytes == 0
    assert not transport.awaited_calls
    pump.reset()
    replacement = _RecordingTransport()
    pump.start(replacement)
    try:
        await pump.enqueue(b"new connection")
        await asyncio.wait_for(pump.join(), 1)
        assert replacement.awaited_calls == [b"new connection"]
        assert not failures
    finally:
        await pump.stop()


class _LatencyFailureBroker(ScriptedBrokerTransport):
    def __init__(self):
        super().__init__()
        self.failure = OSError("latency transport failed")
        self.latency_attempts = []

    def write_nowait(self, data):
        # CONNECT uses the regular scripted transport. A publication lot is
        # observed once before the injected exception; no ACK is manufactured.
        if data[0] >> 4 != 3:
            return False
        self.latency_attempts.append(data)
        raise self.failure


async def test_client_latency_failure_closes_connection_and_settles_batch_receipts():
    broker = _LatencyFailureBroker()
    client = AsyncClient("latency-failure", max_outbound_inflight=20)
    client._transport_factory = transport_factory(broker)
    await client.connect("fake")
    # Prevent the initial per-frame eager attempt so only the aggregate latency
    # shortcut sees the transport failure after all 16 owners are registered.
    client._write_pump._eager_armed = False
    try:
        with pytest.raises(PublishBatchError) as caught:
            await client.publish_many(PublishMessage("batch", b"x", qos=1) for _ in range(16))
        receipt = caught.value.receipt
        assert caught.value.cause is broker.failure
        assert receipt.submitted == 16
        with pytest.raises(PublishBatchError) as completed:
            await asyncio.wait_for(receipt.wait(), 1)
        assert completed.value.cause is broker.failure
        await wait_until(lambda: client._transport is None)
        await asyncio.wait_for(client._write_pump.join(), 1)
        assert len(broker.latency_attempts) == 1
        assert not broker.publishes
        assert client._write_pump.resident_messages == client._write_pump.queued_bytes == 0
        assert not client._receipts and not client._batch_receipts
        assert not client.is_connected
    finally:
        await client.disconnect()
