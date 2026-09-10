"""Configuration atomicity, teardown, reservation, and export contracts."""

from __future__ import annotations

from mqttium.api.async_client import _fifo_register


import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.api.models import PublishBatchReceipt, PublishReceipt
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.errors import PublishBatchError
from mqttium.protocol.engine import (
    EffectKind,
    EngineConfig,
    ProtocolEngine,
    PublishFailure,
)


def _register_publish_handles(
    client: AsyncClient,
    mid: int,
) -> tuple[PublishReceipt, PublishBatchReceipt]:
    receipt = PublishReceipt(
        mid=mid,
        qos=QoS.AT_LEAST_ONCE,
    )
    batch = PublishBatchReceipt()
    batch._register(mid)
    batch._seal()
    _fifo_register(client._receipts, mid, receipt)
    _fifo_register(client._batch_receipts, mid, batch)
    return receipt, batch


async def test_terminal_publish_effect_survives_connection_epoch_change() -> None:
    client = AsyncClient()
    callbacks: list[tuple[int | None, BaseException | None]] = []
    client.on_publish = lambda mid, error: callbacks.append((mid, error))
    receipt, batch = _register_publish_handles(client, 7)

    # SEND is intentionally ordered ahead of completion. If it blocks until the
    # transport epoch changes, the terminal result must still settle locally.
    client._engine._emit(EffectKind.PUBLISH_COMPLETE, 7)
    client._engine._emit(EffectKind.SEND, b"next")
    client._effect_pump.collect_from_engine()
    assert [effect.kind for effect in client._effect_pump.pending] == [
        EffectKind.SEND,
        EffectKind.PUBLISH_COMPLETE,
    ]

    await client._invalidate_connection_epoch()
    client._engine._emit(EffectKind.PINGRESP, None)
    client._effect_pump.collect_from_engine()
    assert [effect.kind for effect in client._effect_pump.pending] == [
        EffectKind.PUBLISH_COMPLETE,
        EffectKind.PINGRESP,
    ]
    assert client._effect_pump.enqueued == client._effect_pump.applied + len(
        client._effect_pump.pending
    )

    await client._effect_pump.drain()
    await client._delivery.callback_queue.join()

    assert receipt.is_done()
    assert batch.is_done()
    assert callbacks == [(7, None)]
    assert 7 not in client._receipts
    assert 7 not in client._batch_receipts
    await client._force_close()


async def test_final_teardown_settles_a_pending_publish_failure() -> None:
    client = AsyncClient()
    callbacks: list[tuple[int | None, BaseException | None]] = []
    client.on_publish = lambda mid, error: callbacks.append((mid, error))
    receipt, batch = _register_publish_handles(client, 9)
    failure = RuntimeError("broker rejected publication")

    client._engine._emit(
        EffectKind.PUBLISH_FAILED,
        PublishFailure(mid=9, reason=failure),
    )
    client._engine._emit(EffectKind.SEND, b"blocked")
    client._effect_pump.collect_from_engine()

    await client._force_close()

    assert receipt.is_done()
    assert receipt._error is failure
    with pytest.raises(PublishBatchError) as exc_info:
        await batch.wait()
    assert exc_info.value.failures[0] is failure
    assert callbacks == [(9, failure)]


def test_engine_config_is_immutable() -> None:
    config = EngineConfig(keepalive=60)
    with pytest.raises(AttributeError):
        config.keepalive = 30
    assert config.keepalive == 60


def test_attached_config_is_immutable() -> None:
    engine = ProtocolEngine()
    original = engine.config.protocol
    with pytest.raises(AttributeError):
        engine.config.protocol = MQTTProtocolVersion.MQTTv5
    assert engine.config.protocol is original


def test_outbound_reservation_underflow_raises_without_corrupting_counters() -> None:
    engine = ProtocolEngine()

    with pytest.raises(AssertionError, match="message reservation underflow"):
        engine.outbound._release_reservation(0)
    assert engine.pending_outbound_messages == 0
    assert engine.pending_outbound_bytes == 0

    engine.outbound._pending_messages = 1
    engine.outbound._pending_bytes = 4
    with pytest.raises(AssertionError, match="byte reservation underflow"):
        engine.outbound._release_reservation(5)
    assert engine.pending_outbound_messages == 1
    assert engine.pending_outbound_bytes == 4
