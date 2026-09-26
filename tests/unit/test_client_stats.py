import asyncio
from dataclasses import FrozenInstanceError

import pytest

from mqttium.api import AsyncClient, ClientStats
from mqttium.api import async_client as async_client_module
from mqttium.enums import ConnectionState, QoS
from mqttium.protocol.effects import EffectKind
from mqttium.transport.stats import TransportStats
from mqttium.types import Message
from tests.support import accept_message, wait_until


def test_initial_stats_snapshot_is_immutable_and_side_effect_free() -> None:
    client = AsyncClient(
        max_write_queue_messages=7,
        max_write_queue_bytes=1234,
        max_iterator_messages=11,
    )

    snapshot = client.stats()

    assert isinstance(snapshot, ClientStats)
    assert snapshot.state is ConnectionState.NEW
    assert snapshot.connections == 0
    assert snapshot.outbound.unacknowledged_messages == 0
    assert snapshot.outbound.unacknowledged_bytes == 0
    assert snapshot.inbound.inflight == 0
    assert snapshot.inbound.replay_pending is False
    assert snapshot.writer.queued_messages == 0
    assert snapshot.writer.queued_bytes == 0
    assert snapshot.writer.message_limit == 7
    assert snapshot.writer.byte_limit == 1234
    assert snapshot.delivery.iterator_limit == 11
    assert snapshot.delivery.iterator_queued == 0
    assert snapshot.decoder.buffered_bytes == 0
    assert snapshot.receipts.publish == 0
    assert snapshot.transport.pending_write_bytes == 0
    tasks = client._running_tasks()
    assert set(tasks) == {
        "reader",
        "writer",
        "keepalive",
        "reconnect",
        "effect_flush",
        "lifecycle",
        "auth",
    }
    assert not any(tasks.values())

    with pytest.raises(FrozenInstanceError):
        snapshot.connections = 1  # type: ignore[misc]


def test_stats_reports_current_state_and_lifetime_high_water_marks() -> None:
    client = AsyncClient(max_write_queue_messages=4, max_write_queue_bytes=1024)

    handle = client._engine.queue_publish("a", b"bc", qos=QoS.AT_LEAST_ONCE)
    assert handle.mid is not None

    client._decoder.feed(b"\x30")
    assert client._write_pump.try_enqueue(b"abcd")
    client._engine._emit(
        EffectKind.MESSAGE,
        Message(topic="in", payload=b"payload", qos=QoS.AT_LEAST_ONCE, mid=1),
        requires_delivery_mark=True,
    )
    client._effect_pump.collect_from_engine()

    loaded = client.stats()
    loaded_effects = client._effect_pump.counters()
    assert loaded.outbound.unacknowledged_messages == 1
    assert loaded.outbound.unacknowledged_bytes == 3
    assert loaded.outbound.unacknowledged_high_water_messages == 1
    assert loaded.outbound.unacknowledged_high_water_bytes == 3
    assert loaded.outbound.awaiting_slot == 1
    assert loaded.outbound.packet_ids_in_use == 1
    assert loaded.writer.queued_messages == 1
    assert loaded.writer.queued_bytes == 4
    assert loaded.writer.high_water_messages == 1
    assert loaded.writer.high_water_bytes == 4
    assert loaded.decoder.buffered_bytes == 1
    assert loaded.decoder.high_water_bytes == 1
    assert loaded_effects["pending"] == 1
    assert loaded_effects["pending_high_water"] == 1

    client._write_pump.discard()
    client._decoder.clear()
    client._effect_pump.discard_connection_effects()
    client._delivery_lane.discard()

    drained = client.stats()
    drained_effects = client._effect_pump.counters()
    assert drained.writer.queued_messages == 0
    assert drained.writer.queued_bytes == 0
    assert drained.writer.high_water_messages == 1
    assert drained.writer.high_water_bytes == 4
    assert drained.decoder.buffered_bytes == 0
    assert drained.decoder.high_water_bytes == 1
    assert drained_effects["pending"] == 0
    assert drained_effects["pending_high_water"] == 1


class _RecordingTransport:
    def __init__(self) -> None:
        self.parts: list[bytes] = []

    async def write(self, data: bytes) -> None:
        self.parts.append(data)

    async def write_many(self, parts: list[bytes]) -> None:
        self.parts.extend(parts)

    async def read(self, n: int = 65536) -> bytes:
        return b""

    async def close(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False


async def test_writer_worker_records_lifetime_high_water_without_enqueue_overhead() -> None:
    client = AsyncClient(max_write_queue_messages=4, max_write_queue_bytes=1024)
    transport = _RecordingTransport()
    client._write_pump.start(transport)
    try:
        await client._write_pump.enqueue(b"abcd")
        await client._write_pump.join()
    finally:
        await client._write_pump.stop()

    snapshot = client.stats()
    assert transport.parts == [b"abcd"]
    assert snapshot.writer.queued_messages == 0
    assert snapshot.writer.queued_bytes == 0
    assert snapshot.writer.high_water_messages == 1
    assert snapshot.writer.high_water_bytes == 4


def test_ingress_batch_limit_is_a_fixed_positive_quantum() -> None:
    """The read-loop byte quantum is not application-tunable and stays 1 MiB."""
    assert async_client_module._MAX_INGRESS_BATCH_BYTES == 1024 * 1024
    with pytest.raises(TypeError, match="max_ingress_batch_bytes"):
        AsyncClient(max_ingress_batch_bytes=2048)  # type: ignore[call-arg]


def test_each_owner_produces_its_own_snapshot() -> None:
    """`stats()` assembles; it does not reach into anyone's private fields."""
    client = AsyncClient()

    snapshot = client.stats()

    assert snapshot.outbound == client._engine.outbound.stats()
    assert snapshot.inbound == client._engine.inbound.stats()
    assert snapshot.writer == client._write_pump.stats()
    assert snapshot.delivery == client._delivery.stats()
    assert snapshot.transport == TransportStats._unavailable(None)


def test_transport_without_a_stats_method_reports_unavailable() -> None:
    client = AsyncClient()
    transport = _RecordingTransport()
    client._transport = transport

    snapshot = client.stats()

    assert snapshot.transport.closing is False
    assert snapshot.transport.pending_write_bytes == 0
    # Unknown, not an empty receive queue.
    assert snapshot.transport.buffered_read_bytes is None


async def test_writer_decision_counters_describe_the_batches_it_wrote() -> None:
    client = AsyncClient(max_write_queue_messages=8, max_write_queue_bytes=4096)
    transport = _RecordingTransport()
    client._write_pump.start(transport)
    try:
        # A segmented item is written apart from the coalesced run around it.
        await client._write_pump.enqueue(b"aa")
        await client._write_pump.enqueue((b"header", b"payload"))
        await client._write_pump.enqueue(b"bb")
        await client._write_pump.join()
    finally:
        await client._write_pump.stop()

    # Scheduling decisions stay on the pump; the snapshot reports occupancy.
    pump = client._write_pump
    assert pump.batches >= 1
    assert pump.batched_items == 3
    assert pump.batched_bytes == len(b"aa") + len(b"headerpayload") + len(b"bb")
    assert pump.segmented_writes == 1
    assert pump.enqueue_suspensions == 0
    assert client.stats().writer.queued_messages == 0


def test_effect_counters_separate_inline_from_reordered_batches() -> None:
    client = AsyncClient()

    # One effect, applied inline: no deque, no reordering.
    client._engine._emit(EffectKind.SEND, b"x")
    client._effect_pump.collect_from_engine()
    inline = client._effect_pump.counters()
    assert inline["batches"] == 1
    assert inline["inline_effects"] == 1
    assert inline["multi_effect_batches"] == 0
    assert inline["reordered_batches"] == 0

    # A batch whose SEND trails a non-SEND has to be partitioned.
    client._engine._emit(
        EffectKind.MESSAGE,
        Message(topic="in", payload=b"payload", qos=QoS.AT_MOST_ONCE),
    )
    client._engine._emit(EffectKind.SEND, b"y")
    client._effect_pump.collect_from_engine()
    reordered = client._effect_pump.counters()
    assert reordered["batches"] == 2
    assert reordered["multi_effect_batches"] == 1
    assert reordered["reordered_batches"] == 1
    assert reordered["enqueued"] == 2


def test_an_already_ordered_batch_is_counted_but_not_reordered() -> None:
    client = AsyncClient()

    client._engine._emit(EffectKind.SEND, b"x")
    client._engine._emit(
        EffectKind.MESSAGE,
        Message(topic="in", payload=b"payload", qos=QoS.AT_MOST_ONCE),
    )
    client._effect_pump.collect_from_engine()

    effects = client._effect_pump.counters()
    assert effects["multi_effect_batches"] == 1
    assert effects["reordered_batches"] == 0


async def test_effect_high_water_retains_combined_protocol_and_delivery_peak() -> None:
    client = AsyncClient(max_iterator_messages=1, max_write_queue_messages=1)
    await accept_message(client._delivery, Message("in", b"first"))
    for body in (b"second", b"third"):
        client._engine._emit(EffectKind.MESSAGE, Message("in", body))
    client._effect_pump.collect_from_engine()
    delivery = asyncio.create_task(client._delivery_lane.drain())
    await wait_until(lambda: client._delivery_lane.active_count == 2)
    assert client._write_pump.try_enqueue(b"occupied")
    client._engine._emit(EffectKind.SEND, b"later")
    client._effect_pump.collect_from_engine()
    delivery.cancel()
    with pytest.raises(asyncio.CancelledError):
        await delivery
    client._effect_pump.discard_connection_effects()
    assert client._effect_pump.counters()["pending"] == 0
    assert client._effect_pump.counters()["pending_high_water"] == 3
    await client._force_close()


async def test_connections_counts_accepted_connacks() -> None:
    from tests.support import ScriptedBrokerTransport, transport_factory

    broker = ScriptedBrokerTransport()
    client = AsyncClient("c", keepalive=0)
    client._transport_factory = transport_factory(broker)
    await client.connect("fake")
    await client.disconnect()
    second = ScriptedBrokerTransport()
    client._transport_factory = transport_factory(second)
    await client.connect("fake")
    try:
        assert client.stats().connections == 2
    finally:
        await client.disconnect()
