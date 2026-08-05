from dataclasses import FrozenInstanceError

import pytest

from mqttium.api import AsyncClient, ClientStats
from mqttium.enums import ConnectionState, QoS
from mqttium.protocol.effects import EffectKind
from mqttium.types import Message


def test_initial_stats_snapshot_is_immutable_and_side_effect_free() -> None:
    client = AsyncClient(
        max_outbound_messages=7,
        max_outbound_bytes=1234,
        max_ingress_batch_bytes=2048,
        max_pending_messages=11,
        max_pending_callbacks=13,
    )

    snapshot = client.stats()

    assert isinstance(snapshot, ClientStats)
    assert snapshot.state is ConnectionState.NEW
    assert snapshot.connection_epoch == 0
    assert snapshot.protocol.pending_outbound_messages == 0
    assert snapshot.protocol.pending_outbound_bytes == 0
    assert snapshot.writer.queued_messages == 0
    assert snapshot.writer.queued_bytes == 0
    assert snapshot.writer.max_messages == 7
    assert snapshot.writer.max_bytes == 1234
    assert snapshot.delivery.iterator_limit == 11
    assert snapshot.delivery.callback_limit == 13
    assert snapshot.decoder.buffered_bytes == 0
    assert snapshot.decoder.ingress_batch_limit_bytes == 2048
    assert snapshot.receipts.publish == 0
    assert snapshot.transport.kind is None
    assert not any(
        (
            snapshot.tasks.reader,
            snapshot.tasks.writer,
            snapshot.tasks.keepalive,
            snapshot.tasks.reconnect,
            snapshot.tasks.effect_flush,
            snapshot.tasks.callback_worker,
        )
    )

    with pytest.raises(FrozenInstanceError):
        snapshot.connection_epoch = 1  # type: ignore[misc]


def test_stats_reports_current_state_and_lifetime_high_water_marks() -> None:
    client = AsyncClient(max_outbound_messages=4, max_outbound_bytes=1024)

    handle = client._engine.queue_publish("a", b"bc", qos=QoS.AT_LEAST_ONCE)
    assert handle.mid is not None

    client._decoder.feed(b"\x30")
    assert client._write_pump.try_enqueue(b"abcd")
    client._engine._emit(
        EffectKind.MESSAGE,
        Message(topic="in", payload=b"payload", qos=QoS.AT_MOST_ONCE),
    )
    client._collect_effects_locked()

    loaded = client.stats()
    assert loaded.protocol.pending_outbound_messages == 1
    assert loaded.protocol.pending_outbound_bytes == 3
    assert loaded.protocol.pending_outbound_high_water_messages == 1
    assert loaded.protocol.pending_outbound_high_water_bytes == 3
    assert loaded.protocol.queued_outbound_messages == 1
    assert loaded.protocol.packet_ids_in_use == 1
    assert loaded.writer.queued_messages == 1
    assert loaded.writer.queued_bytes == 4
    assert loaded.writer.high_water_messages == 1
    assert loaded.writer.high_water_bytes == 4
    assert loaded.decoder.buffered_bytes == 1
    assert loaded.decoder.high_water_bytes == 1
    assert loaded.effects.pending == 1
    assert loaded.effects.pending_high_water == 1

    client._write_pump.discard()
    client._decoder.clear()
    client._discard_connection_effects()

    drained = client.stats()
    assert drained.writer.queued_messages == 0
    assert drained.writer.queued_bytes == 0
    assert drained.writer.high_water_messages == 1
    assert drained.writer.high_water_bytes == 4
    assert drained.decoder.buffered_bytes == 0
    assert drained.decoder.high_water_bytes == 1
    assert drained.effects.pending == 0
    assert drained.effects.pending_high_water == 1


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


@pytest.mark.asyncio
async def test_writer_worker_records_lifetime_high_water_without_enqueue_overhead() -> None:
    client = AsyncClient(max_outbound_messages=4, max_outbound_bytes=1024)
    transport = _RecordingTransport()
    client._write_pump.start(transport)
    try:
        await client._enqueue_outbound(b"abcd")
        await client._write_pump.join()
    finally:
        await client._write_pump.stop()

    snapshot = client.stats()
    assert transport.parts == [b"abcd"]
    assert snapshot.writer.queued_messages == 0
    assert snapshot.writer.queued_bytes == 0
    assert snapshot.writer.high_water_messages == 1
    assert snapshot.writer.high_water_bytes == 4


@pytest.mark.parametrize("value", [0, -1])
def test_client_rejects_invalid_ingress_batch_limit(value: int) -> None:
    with pytest.raises(ValueError, match="max_ingress_batch_bytes"):
        AsyncClient(max_ingress_batch_bytes=value)
