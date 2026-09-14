"""Independent commitment and user-iterator reentry across ready prefixes."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient, PublishMessage
from mqttium.errors import FlowControlError, PublishBatchError
from tests.support import transport_factory
from tests.unit.test_callback_lifecycle_regressions import task_factory as task_factory
from tests.unit.test_guarded_qos0_publication import _WireBroker


@pytest.mark.parametrize("qos", [0, 1])
async def test_source_reentry_preserves_writer_order_and_separate_receipts(qos, task_factory):
    client = AsyncClient("prefix-source-reentry", max_outbound_inflight=20)
    broker = _WireBroker()
    client._transport_factory = transport_factory(broker)
    external = []

    def messages():
        yield PublishMessage("batch", b"first", qos=qos)
        assert not client._engine_lock.locked()
        # The previous item is already owned by the writer before source code
        # can publish independently; no unaccounted SEND prefix crosses next().
        assert not client._engine.has_pending_effects
        assert not client._effect_pump.pending
        external.append(client.publish_nowait("outside", b"middle", qos=1))
        yield PublishMessage("batch", b"last", qos=qos)

    await client.connect("fake")
    try:
        receipt = await client.publish_many(messages())
        await receipt.wait()
        await external[0].wait()
        await client._write_pump.join()
        assert receipt.submitted == receipt.completed == 2
        assert [packet.payload for packet in broker.publishes] == [b"first", b"middle", b"last"]
        assert not client._receipts
        assert not client._batch_receipts
    finally:
        await client.disconnect()


@pytest.mark.parametrize("failure_index", [0, 1, 63, 64, 65])
async def test_iterator_failure_seals_only_committed_prefix_at_driver_boundaries(failure_index):
    client = AsyncClient("prefix-source-failure", max_outbound_messages=128)
    broker = _WireBroker()
    client._transport_factory = transport_factory(broker)
    failure = ValueError("source failed")

    def messages():
        for index in range(failure_index):
            yield PublishMessage("batch", str(index))
        raise failure

    await client.connect("fake")
    try:
        with pytest.raises(PublishBatchError) as caught:
            await client.publish_many(messages())
        assert caught.value.cause is failure
        receipt = caught.value.receipt
        assert receipt._sealed
        assert receipt.submitted == receipt.completed == failure_index
        await receipt.wait()
        await client._write_pump.join()
        assert [packet.payload for packet in broker.publishes] == [
            str(index).encode() for index in range(failure_index)
        ]
    finally:
        await client.disconnect()


async def test_qos1_effect_handoff_exception_keeps_prefix_without_retry(monkeypatch, task_factory):
    client = AsyncClient("prefix-qos1-ambiguous", max_outbound_inflight=20)
    broker = _WireBroker()
    client._transport_factory = transport_factory(broker)
    consumed = []
    attempts = []
    failure = FlowControlError("ambiguous writer outcome")

    def messages():
        for index in range(4):
            consumed.append(index)
            yield PublishMessage("batch", bytes([index]), qos=1)

    await client.connect("fake")
    try:
        original = client._write_pump.try_enqueue

        def accepted_then_raise(item, *, epoch=None):
            attempts.append(item)
            assert original(item, epoch=epoch)
            if len(attempts) == 2:
                raise failure
            return True

        with monkeypatch.context() as patch:
            patch.setattr(client._write_pump, "try_enqueue", accepted_then_raise)
            with pytest.raises(PublishBatchError) as caught:
                await client.publish_many(messages())
        assert caught.value.cause is failure
        receipt = caught.value.receipt
        assert receipt.submitted == 2
        assert consumed == [0, 1]
        assert len(attempts) == 2
        await client._write_pump.join()
        await asyncio.wait_for(receipt.wait(), 2)
        assert receipt.completed == 2
        assert [packet.payload for packet in broker.publishes] == [b"\x00", b"\x01"]
    finally:
        await client.disconnect()
