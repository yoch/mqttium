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


async def test_publish_many_keeps_existing_prefix_registration(monkeypatch):
    client = AsyncClient("batch-stays-general")
    broker = _WireBroker()
    client._transport_factory = transport_factory(broker)

    def forbid_unit_path(*args, **kwargs):
        raise AssertionError("aggregate publication must retain its existing admission path")

    try:
        await client.connect("fake")
        monkeypatch.setattr(client, "_try_direct_qos0_publish", forbid_unit_path)
        receipt = await client.publish_many(PublishMessage("t", bytes([i])) for i in range(3))
        await receipt.wait()
        await client._write_pump.join()
        assert receipt.submitted == receipt.completed == len(broker.publishes) == 3
        assert receipt.pending_count == 0
    finally:
        await client.disconnect()
