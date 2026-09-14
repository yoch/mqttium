"""Independent commitment and source reentry across ready QoS 0 prefixes."""

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


async def test_mixed_qos_keeps_wire_order_and_yields_within_a_long_source():
    from tests.unit.test_publish_many import BatchBrokerTransport

    client = AsyncClient("prefix-mixed-qos", max_outbound_inflight=20)
    broker = BatchBrokerTransport()
    client._transport_factory = transport_factory(broker)
    consumed = []
    heartbeat = []
    levels = [0] * 63 + [1, 0, 2] + [0] * 234

    def messages():
        for index, qos in enumerate(levels):
            consumed.append(index)
            yield PublishMessage("batch", str(index), qos=qos)

    await client.connect("fake")
    try:
        # The source stays ready across both QoS transitions. A cooperative
        # boundary must still let already scheduled loop work run before the
        # remaining source is consumed.
        asyncio.get_running_loop().call_soon(lambda: heartbeat.append(len(consumed)))
        receipt = await client.publish_many(messages())
        await asyncio.wait_for(receipt.wait(), 2)
        await client._write_pump.join()
        assert heartbeat == [256]
        assert receipt.submitted == receipt.completed == len(levels)
        assert [packet.payload for packet in broker.publishes] == [
            str(index).encode() for index in range(len(levels))
        ]
        assert [int(packet.qos) for packet in broker.publishes] == levels
        assert not client._receipts and not client._batch_receipts
    finally:
        await client.disconnect()


@pytest.mark.parametrize("failure_index", [0, 63, 64])
@pytest.mark.parametrize("invalid", [None, PublishMessage("batch", b"invalid", qos=3)])
async def test_validation_failure_does_not_advance_beyond_committed_prefix(failure_index, invalid):
    client = AsyncClient("prefix-invalid", max_outbound_messages=128)
    broker = _WireBroker()
    client._transport_factory = transport_factory(broker)
    consumed = []

    def messages():
        for index in range(failure_index):
            consumed.append(index)
            yield PublishMessage("batch", str(index))
        consumed.append("invalid")
        yield invalid
        consumed.append("must remain unread")
        yield PublishMessage("batch", b"late")

    await client.connect("fake")
    try:
        with pytest.raises(PublishBatchError) as caught:
            await client.publish_many(messages())
        assert isinstance(caught.value.cause, TypeError if invalid is None else ValueError)
        receipt = caught.value.receipt
        assert receipt._sealed
        assert receipt.submitted == receipt.completed == failure_index
        await receipt.wait()
        await client._write_pump.join()
        assert consumed == [*range(failure_index), "invalid"]
        assert [packet.payload for packet in broker.publishes] == [
            str(index).encode() for index in range(failure_index)
        ]
    finally:
        await client.disconnect()


async def test_source_is_not_called_again_after_its_first_exhaustion():
    class CountingSource:
        def __init__(self):
            self.calls = 0

        def __iter__(self):
            return self

        def __next__(self):
            self.calls += 1
            if self.calls > 65:
                raise StopIteration
            return PublishMessage("batch", str(self.calls))

    source = CountingSource()
    client = AsyncClient("prefix-exhaustion")
    broker = _WireBroker()
    client._transport_factory = transport_factory(broker)
    await client.connect("fake")
    try:
        receipt = await client.publish_many(source)
        await receipt.wait()
        await client._write_pump.join()
        assert source.calls == 66
        assert receipt.submitted == receipt.completed == len(broker.publishes) == 65
    finally:
        await client.disconnect()
