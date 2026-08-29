"""Inbound acknowledgement batches settle and dispatch as one bounded pass."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable

from mqttium.api import AsyncClient
from mqttium.api.models import PublishReceipt
from mqttium.enums import ConnectionState, QoS
from mqttium.packets import PubAckPacket, PubCompPacket, PubRecPacket
from mqttium.protocol.effects import EffectKind, EngineEffect
from tests.support import feed_engine


class _OneReadTransport:
    def __init__(self, wire: bytes) -> None:
        self._wire = wire
        self._closing = False

    async def read(self, _n: int) -> bytes:
        if self._wire:
            wire, self._wire = self._wire, b""
            return wire
        self._closing = True
        return b""

    async def close(self) -> None:
        self._closing = True

    def is_closing(self) -> bool:
        return self._closing


async def test_one_tcp_read_batches_puback_and_pubcomp_settlement() -> None:
    client = AsyncClient(client_id="batch-ack-reader")
    engine = client._engine
    engine.state = ConnectionState.CONNECTED

    qos1 = engine.queue_publish("batch/qos1", b"one", qos=QoS.AT_LEAST_ONCE)
    qos2 = engine.queue_publish("batch/qos2", b"two", qos=QoS.EXACTLY_ONCE)
    assert qos1.mid is not None and qos2.mid is not None
    engine.take_effects()  # Test transport does not need the outbound PUBLISH frames.
    feed_engine(engine, PubRecPacket(qos2.mid).encode())
    engine.take_effects()  # Advance QoS 2 to WAIT_PUBCOMP and discard its PUBREL.

    receipts = [
        PublishReceipt(qos1.mid, QoS.AT_LEAST_ONCE),
        PublishReceipt(qos2.mid, QoS.EXACTLY_ONCE),
    ]
    for receipt in receipts:
        assert receipt.mid is not None
        client._register_publish_receipt(receipt.mid, receipt)

    enqueued: list[list[tuple[object, ...]]] = []
    enqueue = client._delivery.enqueue_callback_batch_nowait

    def record_batch(callback: Callable[..., object], args_batch: list[tuple[object, ...]]) -> None:
        enqueued.append(args_batch.copy())
        enqueue(callback, args_batch)

    client._delivery.enqueue_callback_batch_nowait = record_batch
    seen: list[int | None] = []

    async def on_publish(mid: int | None, _reason: BaseException | None) -> None:
        seen.append(mid)

    client.on_publish = on_publish
    client._transport = _OneReadTransport(
        PubAckPacket(qos1.mid).encode() + PubCompPacket(qos2.mid).encode()
    )

    await client._read_loop()

    assert all(receipt.is_done() for receipt in receipts)
    assert enqueued == [[(qos1.mid, None), (qos2.mid, None)]]
    assert seen == [qos1.mid, qos2.mid]


async def test_single_puback_read_keeps_the_ordinary_completion_path() -> None:
    client = AsyncClient(client_id="batch-ack-single-control")
    engine = client._engine
    engine.state = ConnectionState.CONNECTED
    publish = engine.queue_publish("batch/single", b"one", qos=QoS.AT_LEAST_ONCE)
    assert publish.mid is not None
    engine.take_effects()
    receipt = PublishReceipt(publish.mid, QoS.AT_LEAST_ONCE)
    client._register_publish_receipt(publish.mid, receipt)

    def unexpected_batch() -> None:
        raise AssertionError("one PUBACK must not enter ACK batching")

    client._effect_pump.drain_ingress_ack_batch_inline = unexpected_batch
    client._transport = _OneReadTransport(PubAckPacket(publish.mid).encode())

    await client._read_loop()

    assert receipt.is_done()


async def test_terminal_batch_is_one_physical_bounded_callback_job() -> None:
    client = AsyncClient(client_id="batch-ack-physical", max_pending_callbacks=4)
    receipts = [PublishReceipt(mid, QoS.AT_LEAST_ONCE) for mid in (1, 2, 3)]
    for receipt in receipts:
        assert receipt.mid is not None
        client._register_publish_receipt(receipt.mid, receipt)
        client._engine._emit(EffectKind.PUBLISH_COMPLETE, receipt.mid)
    seen: list[int | None] = []

    async def on_publish(mid: int | None, _reason: BaseException | None) -> None:
        seen.append(mid)

    client.on_publish = on_publish

    client._collect_effects_locked()
    client._effect_pump.drain_ingress_ack_batch_inline()

    assert all(receipt.is_done() for receipt in receipts)
    assert len(client._callback_queue._queue) == 1  # type: ignore[attr-defined]
    assert client.stats().delivery.callback_queued == 3
    await client._callback_queue.join()
    assert seen == [1, 2, 3]
    assert client.stats().delivery.callback_queued == 0
    await client._shutdown_callback_worker(drain=False)


async def test_terminal_callback_batch_isolates_each_callback_error() -> None:
    client = AsyncClient(client_id="batch-ack-errors", max_pending_callbacks=4)
    effects = deque(EngineEffect(EffectKind.PUBLISH_COMPLETE, mid) for mid in (1, 2, 3))
    seen: list[int] = []
    errors: list[BaseException] = []

    def callback(mid: int | None, _reason: BaseException | None) -> None:
        assert mid is not None
        if mid == 2:
            raise ValueError("boom")
        seen.append(mid)

    client.on_publish = callback
    client._delivery.report_callback_error = (  # type: ignore[method-assign]
        lambda _callback, exc: errors.append(exc)
    )

    assert client._apply_terminal_effect_batch_inline(effects, client._connection_epoch) == 3
    await client._callback_queue.join()

    assert seen == [1, 3]
    assert len(errors) == 1 and isinstance(errors[0], ValueError)
    await client._shutdown_callback_worker(drain=False)


async def test_shutdown_releases_terminal_batch_callback_capacity() -> None:
    client = AsyncClient(client_id="batch-ack-shutdown", max_pending_callbacks=4)
    effects = deque(EngineEffect(EffectKind.PUBLISH_COMPLETE, mid) for mid in (1, 2, 3))

    async def on_publish(_mid: int | None, _reason: BaseException | None) -> None:
        return None

    client.on_publish = on_publish

    assert client._apply_terminal_effect_batch_inline(effects, client._connection_epoch) == 3
    assert client.stats().delivery.callback_queued == 3
    assert client._callback_queue.maxsize == 2

    await client._shutdown_callback_worker(drain=False)

    assert client._callback_queue.empty()
    assert client.stats().delivery.callback_queued == 0
    assert client._callback_queue.maxsize == 4


async def test_duplicate_mids_settle_receipts_in_per_mid_fifo_order() -> None:
    client = AsyncClient(client_id="batch-ack-duplicate-mid")
    old = PublishReceipt(7, QoS.AT_LEAST_ONCE)
    other = PublishReceipt(8, QoS.AT_LEAST_ONCE)
    reused = PublishReceipt(7, QoS.AT_LEAST_ONCE)
    settled: list[str] = []
    old._on_settle = lambda _receipt: settled.append("old-7")
    other._on_settle = lambda _receipt: settled.append("mid-8")
    reused._on_settle = lambda _receipt: settled.append("reused-7")
    client._register_publish_receipt(7, old)
    client._register_publish_receipt(8, other)
    client._register_publish_receipt(7, reused)
    callbacks: list[int | None] = []
    client.on_publish = lambda mid, _reason: callbacks.append(mid)
    for mid in (7, 8, 7):
        client._engine._emit(EffectKind.PUBLISH_COMPLETE, mid)

    client._collect_effects_locked()
    client._effect_pump.drain_ingress_ack_batch_inline()

    assert settled == ["old-7", "mid-8", "reused-7"]
    assert old.is_done() and other.is_done() and reused.is_done()
    assert client._callback_queue.empty()
    assert callbacks == [7, 8, 7]
    await client._callback_queue.join()
    await client._shutdown_callback_worker(drain=False)


async def test_idle_sync_terminal_batch_dispatches_inline_until_reentrant_queueing() -> None:
    client = AsyncClient(client_id="batch-ack-inline-reentrant", max_pending_callbacks=4)
    effects = deque(EngineEffect(EffectKind.PUBLISH_COMPLETE, mid) for mid in (1, 2, 3))
    receipts = {mid: PublishReceipt(mid, QoS.AT_LEAST_ONCE) for mid in (1, 2, 3)}
    for mid, receipt in receipts.items():
        client._register_publish_receipt(mid, receipt)
    seen: list[int | None] = []

    def callback(mid: int | None, _reason: BaseException | None) -> None:
        seen.append(mid)
        if mid == 1:
            assert client._try_enqueue_callback(callback, 99, None)

    client.on_publish = callback

    assert client._apply_terminal_effect_batch_inline(effects, client._connection_epoch) == 1
    assert seen == [1]
    assert receipts[1].is_done()
    assert not receipts[2].is_done()

    remaining = deque(tuple(effects)[1:])
    assert client._apply_terminal_effect_batch_inline(remaining, client._connection_epoch) == 2
    await client._callback_queue.join()

    assert seen == [1, 99, 2, 3]
    assert all(receipt.is_done() for receipt in receipts.values())
    await client._shutdown_callback_worker(drain=False)


def test_inline_terminal_batch_reloads_a_replaced_callback() -> None:
    client = AsyncClient(client_id="batch-ack-inline-replaced-callback")
    effects = deque(EngineEffect(EffectKind.PUBLISH_COMPLETE, mid) for mid in (1, 2, 3))
    seen: list[tuple[str, int | None]] = []

    def replacement(mid: int | None, _reason: BaseException | None) -> None:
        seen.append(("replacement", mid))

    def initial(mid: int | None, _reason: BaseException | None) -> None:
        seen.append(("initial", mid))
        client.on_publish = replacement

    client.on_publish = initial
    assert client._apply_terminal_effect_batch_inline(effects, client._connection_epoch) == 1
    remaining = deque(tuple(effects)[1:])
    assert client._apply_terminal_effect_batch_inline(remaining, client._connection_epoch) == 2

    assert seen == [("initial", 1), ("replacement", 2), ("replacement", 3)]


def test_general_qos0_effect_drain_never_checks_terminal_batching() -> None:
    client = AsyncClient(client_id="batch-ack-qos0-control")
    client._engine._emit(EffectKind.SEND, b"qos0")
    client._engine._emit(EffectKind.PUBLISH_COMPLETE, None)
    client._collect_effects_locked()

    def unexpected_batch(_effects: object, _epoch: int) -> int:
        raise AssertionError("ordinary QoS 0 drain must not enter ACK batching")

    client._apply_terminal_effect_batch_inline = unexpected_batch  # type: ignore[method-assign]

    client._drain_effects_inline()

    assert not client._pending_effects
    assert client._outbound.get_nowait() == b"qos0"
    client._outbound.task_done()


def test_terminal_batch_preflights_the_whole_logical_callback_bound() -> None:
    client = AsyncClient(client_id="batch-ack-bound", max_pending_callbacks=2)
    receipts = [PublishReceipt(mid, QoS.AT_LEAST_ONCE) for mid in (1, 2, 3)]
    effects = deque()
    for receipt in receipts:
        assert receipt.mid is not None
        client._register_publish_receipt(receipt.mid, receipt)
        effects.append(EngineEffect(EffectKind.PUBLISH_COMPLETE, receipt.mid))

    async def on_publish(_mid: int | None, _reason: BaseException | None) -> None:
        return None

    client.on_publish = on_publish

    applied = client._apply_terminal_effect_batch_inline(effects, client._connection_epoch)

    assert applied == 0
    assert not any(receipt.is_done() for receipt in receipts)
    assert client._callback_queue.empty()
    assert client.stats().delivery.callback_queued == 0
    assert client._callback_queue.maxsize == 2


def test_stale_epoch_cannot_settle_a_terminal_batch_directly() -> None:
    client = AsyncClient(client_id="batch-ack-stale")
    receipts = [PublishReceipt(mid, QoS.AT_LEAST_ONCE) for mid in (1, 2)]
    effects = deque()
    for receipt in receipts:
        assert receipt.mid is not None
        client._register_publish_receipt(receipt.mid, receipt)
        effects.append(EngineEffect(EffectKind.PUBLISH_COMPLETE, receipt.mid))
    client._connection_epoch = 2

    applied = client._apply_terminal_effect_batch_inline(effects, epoch=1)

    assert applied == 0
    assert not any(receipt.is_done() for receipt in receipts)
