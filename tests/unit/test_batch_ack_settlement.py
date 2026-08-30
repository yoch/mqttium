from __future__ import annotations

from mqttium.api import AsyncClient
from mqttium.api.models import PublishReceipt
from mqttium.enums import QoS
from mqttium.protocol.effects import EffectKind, EngineEffect, PublishFailure


def _receipt(client: AsyncClient, mid: int) -> PublishReceipt:
    receipt = PublishReceipt(mid=mid, qos=QoS.AT_LEAST_ONCE)
    client._register_publish_receipt(mid, receipt)
    return receipt


def test_ingress_terminal_batch_settles_and_dispatches_in_effect_order() -> None:
    client = AsyncClient(client_id="batch-ack-order")
    seen: list[tuple[int | None, BaseException | None]] = []
    client.on_publish = lambda mid, reason: seen.append((mid, reason))
    first = _receipt(client, 7)
    second = _receipt(client, 8)
    failure = RuntimeError("rejected")
    client._engine._emit(EffectKind.PUBLISH_COMPLETE, 7)
    client._engine._emit(EffectKind.PUBLISH_COMPLETE, 7)
    client._engine._emit(EffectKind.PUBLISH_FAILED, PublishFailure(8, failure))
    client._collect_effects_locked()

    applied = client._effect_pump.drain_ingress_ack_batch_inline()

    assert applied == 3
    assert seen == [(7, None), (7, None), (8, failure)]
    assert first.is_done()
    assert second.is_done()
    assert not client._effect_pump.pending


def test_ingress_terminal_batch_leaves_single_ack_on_historical_path() -> None:
    client = AsyncClient(client_id="batch-ack-single")
    receipt = _receipt(client, 7)
    client._effect_pump.pending.append(EngineEffect(EffectKind.PUBLISH_COMPLETE, 7))

    applied = client._effect_pump.drain_ingress_ack_batch_inline()

    assert applied == 0
    assert not receipt.is_done()
    assert len(client._effect_pump.pending) == 1


async def test_full_callback_queue_leaves_batch_for_bounded_worker() -> None:
    client = AsyncClient(client_id="batch-ack-full", max_pending_callbacks=1)
    seen: list[int | None] = []

    async def on_publish(mid: int | None, _reason: BaseException | None) -> None:
        seen.append(mid)

    client.on_publish = on_publish
    receipts = [_receipt(client, 7), _receipt(client, 8)]
    client._engine._emit(EffectKind.PUBLISH_COMPLETE, 7)
    client._engine._emit(EffectKind.PUBLISH_COMPLETE, 8)
    client._collect_effects_locked()
    client._callback_queue.put_nowait((lambda: None, (), None))

    applied = client._effect_pump.drain_ingress_ack_batch_inline()

    assert applied == 0
    assert len(client._effect_pump.pending) == 2
    assert not any(receipt.is_done() for receipt in receipts)

    client._callback_queue.get_nowait()
    client._callback_queue.task_done()
    await client._drain_effects()
    await client._callback_queue.join()
    await client._shutdown_callback_worker(drain=False)
    assert seen == [7, 8]
    assert all(receipt.is_done() for receipt in receipts)
