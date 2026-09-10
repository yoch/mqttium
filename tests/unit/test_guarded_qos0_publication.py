"""QoS 0 unit publication keeps one writer and an unambiguous handoff."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

import mqttium.api.async_client as client_module
from mqttium.api import AsyncClient, Properties, PublishMessage
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.errors import FlowControlError
from tests.support import ScriptedBrokerTransport, transport_factory
from tests.unit.test_callback_lifecycle_regressions import task_factory as task_factory


class _WireBroker(ScriptedBrokerTransport):
    def __init__(self, protocol=MQTTProtocolVersion.MQTTv311):
        super().__init__(protocol=protocol)
        self.on_wire = None

    def handle_packet(self, raw):
        if raw.packet_type is PacketType.PUBLISH and self.on_wire is not None:
            self.on_wire()
        super().handle_packet(raw)

    def write_nowait(self, data):
        self.written.append(data)
        self.decoder.feed(data)
        for raw in self.decoder.drain_packets():
            self.handle_packet(raw)
        return True


@pytest.mark.parametrize("nowait", [False, True])
@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
@pytest.mark.parametrize("notification", ["none", "sync", "async"])
async def test_unit_qos0_skips_general_effects_and_receipt_precedes_wire(
    monkeypatch, task_factory, nowait, protocol, notification
):
    client = AsyncClient("direct-unit", protocol=protocol)
    broker = _WireBroker(protocol)
    client._transport_factory = transport_factory(broker)
    seen, created, at_wire = [], [], []

    async def async_callback(mid, error):
        await asyncio.sleep(0)
        seen.append((mid, error))

    if notification == "sync":
        client.on_publish = lambda mid, error: seen.append((mid, error))
    elif notification == "async":
        client.on_publish = async_callback
    real_receipt = client_module.PublishReceipt

    def create_receipt(*args, **kwargs):
        receipt = real_receipt(*args, **kwargs)
        created.append(receipt)
        return receipt

    def forbid_general_queue(self, *args, **kwargs):
        raise AssertionError("ready unit QoS 0 must not create general publication effects")

    try:
        await client.connect("fake")
        monkeypatch.setattr(client_module, "PublishReceipt", create_receipt)
        monkeypatch.setattr(type(client._engine.outbound), "queue_publish", forbid_general_queue)
        broker.on_wire = lambda: at_wire.append((len(created), list(seen)))
        properties = (
            Properties({"user_property": (("key", "value"),)})
            if protocol == MQTTProtocolVersion.MQTTv5
            else None
        )
        if nowait:
            receipt = client.publish_nowait("t", b"x", properties=properties)
        else:
            receipt = await client.publish("t", b"x", properties=properties)
        assert seen == []
        assert created == [receipt]
        assert receipt.mid is None and receipt.is_done()
        assert not client._engine.has_pending_effects
        assert not client._effect_pump.pending
        await client._write_pump.join()
        await client._delivery.callback_queue.join()
        assert len(broker.publishes) == 1
        assert at_wire == [(1, [])]
        assert seen == ([] if notification == "none" else [(None, None)])
    finally:
        await client.disconnect()


@pytest.mark.parametrize("nowait", [False, True])
async def test_writer_exception_after_handoff_is_never_retried(monkeypatch, nowait):
    client = AsyncClient("exception-after-wire")
    broker = _WireBroker()
    client._transport_factory = transport_factory(broker)
    calls = []
    seen = []
    client.on_publish = lambda *args: seen.append(args)
    try:
        await client.connect("fake")
        enqueue = client._write_pump.try_enqueue

        def ambiguous_failure(item, *, epoch=None):
            calls.append(item)
            assert enqueue(item, epoch=epoch)
            raise FlowControlError("failed after writer handoff")

        with monkeypatch.context() as patch:
            patch.setattr(client._write_pump, "try_enqueue", ambiguous_failure)
            with pytest.raises(FlowControlError, match="after writer handoff"):
                if nowait:
                    client.publish_nowait("t", b"one")
                else:
                    await client.publish("t", b"one")
        await client._write_pump.join()
        assert len(calls) == len(broker.publishes) == 1
        assert not seen
        assert not client._engine.has_pending_effects
        assert not client._effect_pump.pending
    finally:
        await client.disconnect()


async def test_async_clean_writer_refusal_uses_existing_admission_path(monkeypatch):
    client = AsyncClient("writer-clean-refusal")
    broker = _WireBroker()
    client._transport_factory = transport_factory(broker)
    attempts = []
    try:
        await client.connect("fake")
        enqueue = client._write_pump.try_enqueue

        def refuse_first(item, *, epoch=None):
            attempts.append(item)
            return False if len(attempts) == 1 else enqueue(item, epoch=epoch)

        monkeypatch.setattr(client._write_pump, "try_enqueue", refuse_first)
        receipt = await asyncio.wait_for(client.publish("t", b"one"), 1)
        await client._write_pump.join()
        assert receipt.is_done()
        assert len(attempts) == 2
        assert len(broker.publishes) == 1
    finally:
        await client.disconnect()


async def test_nowait_clean_refusal_does_not_commit_alias_or_notification(monkeypatch):
    client = AsyncClient("nowait-clean-refusal", protocol=MQTTProtocolVersion.MQTTv5)
    broker = _WireBroker(MQTTProtocolVersion.MQTTv5)
    client._transport_factory = transport_factory(broker)
    notifications = []
    client.on_publish = lambda *args: notifications.append(args)
    try:
        await client.connect("fake")
        client._engine.negotiated = replace(client.negotiated, topic_alias_maximum=2)
        with monkeypatch.context() as patch:
            patch.setattr(client._write_pump, "try_enqueue", lambda *args, **kwargs: False)
            with pytest.raises(FlowControlError):
                client.publish_nowait("t", b"one", properties=Properties({"topic_alias": 1}))
        assert client._engine.outbound._topic_aliases == {}
        assert not broker.publishes
        assert not client._engine.has_pending_effects
        assert not client._effect_pump.pending
        assert client._delivery.callback_queue.empty()
        assert not notifications
    finally:
        await client.disconnect()


async def test_alias_is_committed_only_after_direct_writer_acceptance(monkeypatch):
    client = AsyncClient("alias-boundary", protocol=MQTTProtocolVersion.MQTTv5)
    broker = _WireBroker(MQTTProtocolVersion.MQTTv5)
    client._transport_factory = transport_factory(broker)
    try:
        await client.connect("fake")
        client._engine.negotiated = replace(client.negotiated, topic_alias_maximum=2)
        enqueue = client._write_pump.try_enqueue
        aliases_at_handoff = []

        def observe(item, *, epoch=None):
            aliases_at_handoff.append(dict(client._engine.outbound._topic_aliases))
            return enqueue(item, epoch=epoch)

        with monkeypatch.context() as patch:
            patch.setattr(client._write_pump, "try_enqueue", observe)
            await client.publish("t", b"one", properties=Properties({"topic_alias": 1}))
        assert aliases_at_handoff == [{}]
        assert client._engine.outbound._topic_aliases == {1: "t"}
    finally:
        await client.disconnect()


async def test_segmented_unit_qos0_stays_in_writer_queue():
    client = AsyncClient("segmented-direct", max_outbound_bytes=1024)
    broker = _WireBroker()
    client._transport_factory = transport_factory(broker)
    payload = b"x" * (256 * 1024)
    try:
        await client.connect("fake")
        eager_before = client.stats().writer.eager_writes
        broker.written.clear()
        receipt = client.publish_nowait("t", payload)
        assert receipt.is_done()
        assert client.stats().writer.eager_writes == eager_before
        assert client._write_pump.resident_messages == 1
        with pytest.raises(FlowControlError):
            client.publish_nowait("t", b"next")
        await client._write_pump.join()
        assert len(broker.written) == 2
        assert broker.written[1] is payload
        assert len(broker.publishes) == 1
        assert broker.publishes[0].payload == payload
        assert client._write_pump.resident_messages == 0
    finally:
        await client.disconnect()


async def test_publish_many_shares_direct_path_without_unit_receipts(monkeypatch):
    client = AsyncClient("batch-direct")
    broker = _WireBroker()
    client._transport_factory = transport_factory(broker)

    def forbid_unit_receipt(*args, **kwargs):
        raise AssertionError("batch must not allocate per-item receipts")

    try:
        await client.connect("fake")
        monkeypatch.setattr(client_module, "PublishReceipt", forbid_unit_receipt)
        receipt = await client.publish_many(PublishMessage("t", bytes([i])) for i in range(3))
        await receipt.wait()
        await client._write_pump.join()
        assert receipt.submitted == receipt.completed == len(broker.publishes) == 3
        assert receipt.pending_count == 0
    finally:
        await client.disconnect()


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
@pytest.mark.parametrize("notification", ["none", "sync", "async"])
async def test_batch_registration_precedes_wire_without_unit_receipts(
    monkeypatch, task_factory, protocol, notification
):
    from mqttium.api.models import PublishBatchReceipt

    client = AsyncClient("batch-before-wire", protocol=protocol)
    broker = _WireBroker(protocol)
    client._transport_factory = transport_factory(broker)
    batch_seen, at_wire, notifications = [], [], []
    original_register = PublishBatchReceipt._register

    def register(batch, mid):
        batch_seen.append(batch)
        original_register(batch, mid)

    def callback(mid, reason):
        assert not client._engine_lock.locked()
        notifications.append((mid, reason))

    async def async_callback(mid, reason):
        callback(mid, reason)

    if notification != "none":
        client.on_publish = callback if notification == "sync" else async_callback

    def no_unit(*args, **kwargs):
        raise AssertionError("QoS 0 batch allocated an individual receipt")

    def no_general(*args, **kwargs):
        raise AssertionError("ready batch item used general publication effects")

    await client.connect("fake")
    try:
        monkeypatch.setattr(PublishBatchReceipt, "_register", register)
        monkeypatch.setattr(client_module, "PublishReceipt", no_unit)
        monkeypatch.setattr(type(client._engine.outbound), "queue_publish", no_general)
        broker.on_wire = lambda: at_wire.append(batch_seen[-1].submitted)
        receipt = await client.publish_many(PublishMessage("t", bytes([i])) for i in range(8))
        assert notifications == []
        await client._write_pump.join()
        await client._delivery.callback_queue.join()
        assert receipt.submitted == receipt.completed == 8
        assert all(batch is receipt for batch in batch_seen)
        assert all(count >= index for index, count in enumerate(at_wire, 1))
        assert len(at_wire) == 8
        assert [packet.payload for packet in broker.publishes] == [bytes([i]) for i in range(8)]
        assert notifications == ([] if notification == "none" else [(None, None)] * 8)
    finally:
        await client.disconnect()


@pytest.mark.parametrize("fault", [FlowControlError, OSError])
async def test_batch_partial_handoff_failure_keeps_prefix_and_never_retries(
    monkeypatch, task_factory, fault
):
    from mqttium.errors import PublishBatchError

    client = AsyncClient("batch-partial-write")
    broker = _WireBroker()
    client._transport_factory = transport_factory(broker)
    consumed, attempts, notifications = [], [], []
    client.on_publish = lambda *args: notifications.append(args)

    def messages():
        for index in range(4):
            consumed.append(index)
            yield PublishMessage("t", bytes([index]))

    await client.connect("fake")
    try:
        original = client._write_pump.try_enqueue
        cause = fault("ambiguous write")

        def handoff_then_raise(item, *, epoch=None):
            attempts.append(item)
            accepted = original(item, epoch=epoch)
            assert accepted
            if len(attempts) == 2:
                raise cause
            return accepted

        with monkeypatch.context() as patch:
            patch.setattr(client._write_pump, "try_enqueue", handoff_then_raise)
            with pytest.raises(PublishBatchError) as caught:
                await client.publish_many(messages())
        receipt = caught.value.receipt
        assert caught.value.cause is cause
        assert receipt.submitted == receipt.completed == 2
        assert consumed == [0, 1]
        assert len(attempts) == 2
        await client._write_pump.join()
        await client._delivery.callback_queue.join()
        assert [packet.payload for packet in broker.publishes] == [b"\x00", b"\x01"]
        assert notifications == [(None, None)]
        # The raised submission error reports the ambiguous handoff; the
        # attached receipt describes its already committed prefix, as before.
        await receipt.wait()
    finally:
        await client.disconnect()


async def test_batch_clean_refusal_rolls_back_registration_before_fallback(monkeypatch):
    from mqttium.api.models import PublishBatchReceipt

    protocol = MQTTProtocolVersion.MQTTv5
    client = AsyncClient("batch-clean-refusal", protocol=protocol)
    broker = _WireBroker(protocol)
    client._transport_factory = transport_factory(broker)
    batch_seen, attempts = [], []
    original_register = PublishBatchReceipt._register

    def register(batch, mid):
        original_register(batch, mid)
        batch_seen.append(batch)

    await client.connect("fake")
    try:
        client._engine.negotiated = replace(client.negotiated, topic_alias_maximum=2)
        original_enqueue = client._write_pump.try_enqueue
        original_queue = type(client._engine.outbound).queue_publish

        def enqueue(item, *, epoch=None):
            attempts.append(item)
            if len(attempts) == 1:
                assert batch_seen[-1].submitted == 1
                assert client._engine.outbound._topic_aliases == {}
                return False
            return original_enqueue(item, epoch=epoch)

        def general(session, *args, **kwargs):
            assert batch_seen[-1].submitted == 0
            assert session._topic_aliases == {}
            return original_queue(session, *args, **kwargs)

        monkeypatch.setattr(PublishBatchReceipt, "_register", register)
        monkeypatch.setattr(client._write_pump, "try_enqueue", enqueue)
        monkeypatch.setattr(type(client._engine.outbound), "queue_publish", general)
        receipt = await client.publish_many(
            [
                PublishMessage("alias/topic", b"first", properties=Properties({"topic_alias": 1})),
                PublishMessage("", b"second", properties=Properties({"topic_alias": 1})),
            ]
        )
        await receipt.wait()
        await client._write_pump.join()
        assert receipt.submitted == receipt.completed == 2
        assert len(attempts) == 3
        assert len(broker.publishes) == 2
        assert client._engine.outbound._topic_aliases == {1: "alias/topic"}
    finally:
        await client.disconnect()


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
async def test_direct_batch_keeps_mixed_qos_order_with_tight_bounds(task_factory, protocol):
    from tests.unit.test_publish_many import BatchBrokerTransport

    client = AsyncClient(
        "mixed-direct",
        protocol=protocol,
        max_outbound_inflight=1,
        max_outbound_messages=1,
        max_outbound_bytes=64,
    )
    broker = BatchBrokerTransport(protocol)
    client._transport_factory = transport_factory(broker)
    await client.connect("fake")
    try:
        requests = [PublishMessage("mixed", bytes([i]), qos=i % 3) for i in range(18)]
        receipt = await asyncio.wait_for(client.publish_many(requests), 2)
        await asyncio.wait_for(receipt.wait(), 2)
        await client._write_pump.join()
        assert [message.payload for message in broker.publishes] == [bytes([i]) for i in range(18)]
        assert receipt.submitted == receipt.completed == 18
        assert receipt.pending_count == 0
        assert client.stats().outbound.pending_messages == 0
    finally:
        await client.disconnect()
