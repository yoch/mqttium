"""Experimental public contracts across connection and admission boundaries."""

from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError

import pytest

from mqttium.api import AsyncClient, Properties, PublishMessage, ReconnectPolicy
from mqttium.codec.buffer import RawPacket
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.errors import MQTTError, PublishBatchError
from mqttium.packets import PubAckPacket, PublishPacket, encode_frame
from mqttium.persistence import MemoryInflightStore, SqliteInflightStore
from tests.support import ScriptedBrokerTransport, transport_factory, wait_until


def assert_routes_frozen(client):
    with pytest.raises(MQTTError, match="frozen"):
        client.on_message = lambda _: None
    with pytest.raises(MQTTError, match="frozen"):
        client.message_callback_add("other/+", lambda _: None)
    with pytest.raises(MQTTError, match="frozen"):
        client.message_callback_remove("route/#")


async def test_routes_freeze_during_first_attempt_and_stay_frozen_after_failure() -> None:
    client = AsyncClient("freeze", message_delivery="callback")
    client.message_callback_add("route/#", lambda _: None)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def fail(*args, **kwargs):
        entered.set()
        await release.wait()
        raise OSError("transport unavailable")

    client._transport_factory = fail
    attempt = asyncio.create_task(client.connect("fake"))
    await entered.wait()
    assert_routes_frozen(client)
    release.set()
    with pytest.raises(OSError):
        await attempt
    assert_routes_frozen(client)
    await client.disconnect()
    assert_routes_frozen(client)
    broker = ScriptedBrokerTransport()
    client._transport_factory = transport_factory(broker)
    await client.connect("fake")
    assert_routes_frozen(client)
    # Broker subscription intent remains independently changeable.
    await client.subscribe("another/topic", qos=1)
    await client.disconnect()
    assert_routes_frozen(client)


async def test_routes_stay_frozen_through_automatic_reconnect() -> None:
    client = AsyncClient(
        "reconnect-routes",
        message_delivery="callback",
        reconnect=ReconnectPolicy(initial_delay=0.001, max_delay=0.001),
    )
    client.message_callback_add("route/#", lambda _: None)
    brokers = [ScriptedBrokerTransport(), ScriptedBrokerTransport()]
    calls = 0
    connected = asyncio.Event()
    seen = 0

    async def factory(*args, **kwargs):
        nonlocal calls
        transport = brokers[calls]
        calls += 1
        return transport

    def on_connect(_packet):
        nonlocal seen
        seen += 1
        if seen == 2:
            connected.set()

    client._transport_factory = factory
    client.on_connect = on_connect
    try:
        await client.connect("fake")
        await client._delivery.callback_queue.join()
        await brokers[0].close()
        await asyncio.wait_for(connected.wait(), 2)
        assert calls == 2
        assert_routes_frozen(client)
    finally:
        await client.disconnect()
    assert_routes_frozen(client)


def test_reconnect_policy_is_immutable_and_progress_is_per_client() -> None:
    policy = ReconnectPolicy()
    left = AsyncClient(reconnect=policy)
    right = AsyncClient(reconnect=policy)
    with pytest.raises(FrozenInstanceError):
        policy.initial_delay = 2
    left._reconnect.next_delay()
    assert left.stats().reconnect_attempt == 1
    assert right.stats().reconnect_attempt == 0
    assert not hasattr(policy, "attempt")
    assert not hasattr(policy, "next_delay")


def test_auth_handler_is_read_only() -> None:
    async def handler(_packet):
        return None

    client = AsyncClient(auth_handler=handler)
    with pytest.raises(AttributeError):
        client.auth_handler = None
    assert client.auth_handler is handler


class HeldAckBroker(ScriptedBrokerTransport):
    """Hold QoS 1 ACKs so admission boundaries are observable."""

    def handle_packet(self, raw: RawPacket) -> None:
        if raw.packet_type is PacketType.PUBLISH:
            self.publishes.append(PublishPacket.decode(raw.flags, raw.remaining, self.protocol))
        else:
            super().handle_packet(raw)

    def ack(self, index):
        mid = self.publishes[index].mid
        assert mid is not None
        self.push_rx(PubAckPacket(mid).encode(self.protocol))


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
async def test_progressive_batch_reads_one_ahead_and_can_exceed_one_message_budget(
    tmp_path, backend
) -> None:
    store = (
        MemoryInflightStore() if backend == "memory" else SqliteInflightStore(tmp_path / "one.db")
    )
    client = AsyncClient(
        "one", store=store, max_outbound_inflight=1, max_pending_outbound_messages=1
    )
    broker = HeldAckBroker()
    client._transport_factory = transport_factory(broker)
    consumed = []

    def messages():
        for index in range(8):
            consumed.append(index)
            yield PublishMessage("batch", str(index), qos=1)

    await client.connect("fake")
    task = asyncio.create_task(client.publish_many(messages()))
    try:
        for index in range(8):
            await wait_until(lambda index=index: len(broker.publishes) == index + 1)
            assert len(consumed) <= index + 2
            assert client.stats().outbound.pending_messages == 1
            assert client.stats().receipts.publish == 0
            broker.ack(index)
        receipt = await asyncio.wait_for(task, 2)
        await asyncio.wait_for(receipt.wait(), 2)
        assert receipt.submitted == receipt.completed == 8
        assert receipt.pending_count == 0
        assert [p.payload for p in broker.publishes] == [str(i).encode() for i in range(8)]
    finally:
        if not task.done():
            task.cancel()
        await client.disconnect()
        if backend == "sqlite":
            store.close()


async def test_generator_failure_returns_sealed_committed_prefix() -> None:
    client = AsyncClient("prefix", max_outbound_inflight=2)
    broker = HeldAckBroker()
    client._transport_factory = transport_factory(broker)
    failure = RuntimeError("source stopped")

    def source():
        yield PublishMessage("batch", b"accepted", qos=1)
        raise failure

    await client.connect("fake")
    try:
        with pytest.raises(PublishBatchError) as caught:
            await client.publish_many(source())
        receipt = caught.value.receipt
        assert caught.value.cause is failure
        assert receipt._sealed
        assert receipt.submitted == 1
        await wait_until(lambda: len(broker.publishes) == 1)
        broker.ack(0)
        await asyncio.wait_for(receipt.wait(), 2)
        assert receipt.completed == 1
    finally:
        await client.disconnect()


async def test_batch_cancellation_during_flow_wait_seals_active_prefix() -> None:
    client = AsyncClient("cancel-prefix", max_outbound_inflight=1)
    broker = HeldAckBroker()
    client._transport_factory = transport_factory(broker)
    await client.connect("fake")
    requests = (PublishMessage("batch", b"x", qos=1) for _ in range(3))
    task = asyncio.create_task(client.publish_many(requests))
    try:
        await wait_until(lambda: len(broker.publishes) == 1)
        receipt = next(iter(client._batch_receipts.values()))
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert receipt._sealed
        assert receipt.submitted == 1
        assert client.stats().outbound.pending_messages == 1
        broker.ack(0)
        await asyncio.wait_for(receipt.wait(), 2)
        assert len(broker.publishes) == 1
    finally:
        await client.disconnect()


@pytest.mark.parametrize("limit", [None, -1, 1.5, float("inf"), True])
async def test_batch_detail_limit_is_a_finite_nonnegative_integer(limit) -> None:
    client = AsyncClient()
    consumed = []

    def source():
        consumed.append(True)
        yield PublishMessage("t", b"x")

    with pytest.raises(ValueError):
        await client.publish_many(source(), max_failure_details=limit)
    assert not consumed


class ResumedBroker(ScriptedBrokerTransport):
    def handle_packet(self, raw):
        if raw.packet_type is PacketType.CONNECT:
            self.push_rx(encode_frame(PacketType.CONNACK, 0, b"\x01\x00\x00"))
        else:
            super().handle_packet(raw)


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
async def test_properties_and_payload_keep_owned_values_through_restart(tmp_path, backend) -> None:
    path = tmp_path / "owned.db"
    store = MemoryInflightStore() if backend == "memory" else SqliteInflightStore(path)
    client = AsyncClient(
        "owned", clean_start=False, protocol=MQTTProtocolVersion.MQTTv5, store=store
    )
    binary = bytearray(b"original")
    pairs = [["key", "value"]]
    source = {"correlation_data": memoryview(binary), "user_property": pairs}
    props = Properties(source)
    payload = bytearray(b"payload")
    initial = HeldAckBroker(protocol=MQTTProtocolVersion.MQTTv5)
    client._transport_factory = transport_factory(initial)
    await client.connect("fake")
    await client.publish("owned", payload, qos=1, properties=props)
    await wait_until(lambda: bool(initial.publishes))
    await client.disconnect()
    binary[:] = b"modified"
    pairs[0][1] = "changed"
    pairs.append(["extra", "pair"])
    source.clear()
    payload[:] = b"changed"
    if backend == "sqlite":
        store.close()
        store = SqliteInflightStore(path)
    recovered = AsyncClient(
        "owned", clean_start=False, protocol=MQTTProtocolVersion.MQTTv5, store=store
    )
    broker = ResumedBroker(protocol=MQTTProtocolVersion.MQTTv5)
    recovered._transport_factory = transport_factory(broker)
    try:
        await recovered.connect("fake")
        await wait_until(lambda: bool(broker.publishes))
        packet = broker.publishes[0]
        assert packet.payload == b"payload"
        assert packet.properties.get("correlation_data") == b"original"
        assert packet.properties.get("user_property") == (("key", "value"),)
        await wait_until(lambda: recovered.stats().outbound.pending_messages == 0)
    finally:
        await recovered.disconnect()
        if backend == "sqlite":
            store.close()


async def test_batch_settles_old_completion_before_mid_reuse_across_blocked_delivery() -> None:
    client = AsyncClient("mid-reuse", max_outbound_inflight=2, max_pending_messages=1)
    broker = HeldAckBroker()
    client._transport_factory = transport_factory(broker)
    await client.connect("fake")
    inbound = PublishPacket(
        topic="in", payload=b"occupied", qos=0, retain=False, dup=False
    ).encode()
    broker.push_rx(inbound)
    await wait_until(lambda: client.stats().delivery.iterator_queued == 1)
    requests = [PublishMessage("out", str(index), qos=1) for index in range(3)]
    task = asyncio.create_task(client.publish_many(requests))
    try:
        await wait_until(lambda: len(broker.publishes) == 2)
        first, second = broker.publishes
        broker.push_rx(
            PubAckPacket(first.mid).encode() + inbound + PubAckPacket(second.mid).encode()
        )
        # The first terminal effect wakes batch admission. Delivery blocks the
        # second, although the protocol engine has already released both IDs.
        await wait_until(lambda: client.stats().outbound.pending_messages == 0)
        await asyncio.sleep(0)
        assert len(broker.publishes) == 2
        assert not task.done()
        stream = client.messages()
        assert (await anext(stream)).payload == b"occupied"
        await wait_until(lambda: len(broker.publishes) == 3)
        receipt = await asyncio.wait_for(task, 2)
        assert receipt.submitted == 3
        assert receipt.pending_count == 1
        assert not receipt.is_done()
        assert client.stats().outbound.pending_messages == 1
        assert (await anext(stream)).payload == b"occupied"
        broker.ack(2)
        await asyncio.wait_for(receipt.wait(), 2)
        assert client.stats().outbound.pending_messages == 0
        await stream.aclose()
    finally:
        if not task.done():
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        await client.disconnect()
