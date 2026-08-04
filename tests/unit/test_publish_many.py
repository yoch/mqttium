"""Correctness and lifecycle tests for aggregate batch publishing."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from mqttium.api import AsyncClient, PublishMessage
from mqttium.api.models import PublishBatchReceipt
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import PublishBatchError
from mqttium.packets import (
    PubAckPacket,
    PubCompPacket,
    PublishPacket,
    PubRecPacket,
    PubRelPacket,
    encode_frame,
)
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.engine import EngineConfig, ProtocolEngine


class BatchBrokerTransport:
    def __init__(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> None:
        self.protocol = protocol
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False
        self.publishes: list[PublishPacket] = []

    async def write(self, data: bytes) -> None:
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets(limit=10_000):
            if raw.packet_type is PacketType.CONNECT:
                body = (
                    b"\x00\x00\x00" if self.protocol is MQTTProtocolVersion.MQTTv5 else b"\x00\x00"
                )
                self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, body))
            elif raw.packet_type is PacketType.PUBLISH:
                packet = PublishPacket.decode(raw.flags, raw.remaining, self.protocol)
                self.publishes.append(packet)
                if packet.qos is QoS.AT_LEAST_ONCE:
                    assert packet.mid is not None
                    self._rx.put_nowait(PubAckPacket(packet.mid).encode(self.protocol))
                elif packet.qos is QoS.EXACTLY_ONCE:
                    assert packet.mid is not None
                    self._rx.put_nowait(PubRecPacket(packet.mid).encode(self.protocol))
            elif raw.packet_type is PacketType.PUBREL:
                rel = PubRelPacket.decode(raw.remaining, self.protocol)
                self._rx.put_nowait(PubCompPacket(rel.mid).encode(self.protocol))

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        self._closing = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing


def client_with_broker(
    *,
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    window: int = 16,
) -> tuple[AsyncClient, BatchBrokerTransport]:
    client = AsyncClient(
        client_id="batch-test",
        protocol=protocol,
        max_outbound_inflight=window,
    )
    broker = BatchBrokerTransport(protocol)

    async def factory(host: str, port: int, *, ssl: object = None) -> BatchBrokerTransport:
        return broker

    client._transport_factory = factory
    return client, broker


async def test_publish_many_qos0_uses_immediate_aggregate_receipt() -> None:
    client, broker = client_with_broker()
    await client.connect("fake", timeout=2.0)
    receipt = await client.publish_many(
        [PublishMessage("batch/q0", str(index), qos=0) for index in range(200)],
        chunk_size=32,
    )
    await asyncio.wait_for(client._outbound.join(), timeout=2.0)
    assert receipt.submitted == 200
    assert receipt.completed == 200
    assert receipt.pending_count == 0
    assert receipt.is_done()
    await receipt.wait()
    assert len(broker.publishes) == 200
    await client.disconnect()


@pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
async def test_publish_many_qos_acknowledged_without_per_message_waiters(qos: QoS) -> None:
    client, broker = client_with_broker(window=8)
    await client.connect("fake", timeout=2.0)
    receipt = await client.publish_many(
        [PublishMessage("batch/ack", b"x", qos=qos) for _ in range(160)],
        chunk_size=24,
    )
    await asyncio.wait_for(receipt.wait(), timeout=5.0)
    assert receipt.submitted == 160
    assert receipt.completed == 160
    assert receipt.pending_count == 0
    assert not client._receipts
    assert not client._batch_receipts
    assert len(broker.publishes) == 160
    await client.disconnect()


def test_engine_chunk_rollback_restores_all_mutable_state() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="rollback"))
    engine.state = ConnectionState.CONNECTED

    with pytest.raises(Exception):
        engine.queue_publish_many(
            [
                ("valid/topic", b"x", QoS.AT_LEAST_ONCE, False, None),
                ("invalid/#", b"x", QoS.AT_LEAST_ONCE, False, None),
            ]
        )

    assert not engine.take_effects()
    assert not list(engine.store.out_items())
    assert not engine._queued
    assert engine.flow.inflight == 0
    assert len(engine.packet_ids) == 0


def test_engine_chunk_uses_one_sqlite_commit(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "batch-publish.db")
    engine = ProtocolEngine(EngineConfig(client_id="sqlite-batch"), store=store)
    engine.state = ConnectionState.CONNECTED
    trace: list[str] = []
    store._conn.set_trace_callback(trace.append)

    handles = engine.queue_publish_many(
        [(f"batch/{index}", b"x", QoS.AT_LEAST_ONCE, False, None) for index in range(20)]
    )

    assert len(handles) == 20
    assert sum(statement == "COMMIT" for statement in trace) == 1
    store.close()


async def test_batch_receipt_aggregates_failures() -> None:
    receipt = PublishBatchReceipt()
    receipt._register(7)
    receipt._register(8)
    receipt._complete(7)
    failure = RuntimeError("rejected")
    receipt._complete(8, failure)
    receipt._seal()

    with pytest.raises(PublishBatchError) as raised:
        await receipt.wait()
    assert raised.value.failures == {1: failure}


def test_batch_receipt_failure_indexes_survive_mid_reuse() -> None:
    receipt = PublishBatchReceipt()
    receipt._register(7)
    receipt._complete(7)
    receipt._register(7)
    failure = RuntimeError("second use rejected")
    receipt._complete(7, failure)
    receipt._seal()

    assert receipt.completed == 2
    assert receipt.failures == {1: failure}


def test_batch_receipt_does_not_retain_completed_mid_history() -> None:
    receipt = PublishBatchReceipt()
    for index in range(10_000):
        mid = index % 65_535 + 1
        receipt._register(mid)
        receipt._complete(mid)
    receipt._seal()

    assert receipt.submitted == 10_000
    assert receipt.completed == 10_000
    assert receipt.pending_count == 0
    assert not hasattr(receipt, "mids")
    assert not hasattr(receipt, "_mids")


def test_batch_receipt_bounds_failure_details_and_keeps_totals() -> None:
    receipt = PublishBatchReceipt(max_failure_details=2)
    failures = [RuntimeError("one"), ValueError("two"), RuntimeError("three")]
    for mid in range(1, 4):
        receipt._register(mid)
    for mid, failure in enumerate(failures, start=1):
        receipt._complete(mid, failure)
    receipt._seal()

    assert receipt.failure_count == 3
    assert len(receipt.failures) == 2
    assert receipt.failure_counts == {"RuntimeError": 2, "ValueError": 1}


async def test_batch_receipt_failure_sink_receives_every_failure() -> None:
    streamed: list[tuple[int, BaseException]] = []
    receipt = PublishBatchReceipt(
        max_failure_details=1,
        failure_sink=lambda index, error: streamed.append((index, error)),
    )
    for mid in range(1, 4):
        receipt._register(mid)
        receipt._complete(mid, RuntimeError(str(mid)))
    receipt._seal()

    with pytest.raises(PublishBatchError) as raised:
        await receipt.wait()

    assert [index for index, _error in streamed] == [0, 1, 2]
    assert raised.value.failure_count == 3
    assert raised.value.failure_counts == {"RuntimeError": 3}
    assert len(raised.value.failures) == 1


def test_batch_receipt_can_disable_failure_detail_retention() -> None:
    receipt = PublishBatchReceipt(max_failure_details=0)
    receipt._register(1)
    receipt._complete(1, RuntimeError("failure"))
    receipt._seal()

    assert receipt.failure_count == 1
    assert receipt.failures == {}
