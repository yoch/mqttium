"""Regression coverage for the PR 18 design-audit follow-ups."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.api.models import PublishBatchReceipt, PublishReceipt
from mqttium.compat.paho import Client
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.errors import PublishBatchError
from mqttium.persistence import MemoryInflightStore, PagedInflightStore
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
        _event=asyncio.Event(),
    )
    batch = PublishBatchReceipt()
    batch._register(mid)
    batch._seal()
    client._register_publish_receipt(mid, receipt)
    client._register_batch_receipt(mid, batch)
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
    client._collect_effects_locked()
    assert [effect.kind for effect in client._pending_effects] == [
        EffectKind.SEND,
        EffectKind.PUBLISH_COMPLETE,
    ]

    await client._invalidate_connection_epoch()
    client._engine._emit(EffectKind.PINGRESP, None)
    client._collect_effects_locked()
    assert [effect.kind for effect in client._pending_effects] == [
        EffectKind.PUBLISH_COMPLETE,
        EffectKind.PINGRESP,
    ]
    assert client._effect_enqueued == client._effect_applied + len(client._pending_effects)

    await client._drain_effects()
    await asyncio.sleep(0)

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
    client._collect_effects_locked()

    await client._force_close()

    assert receipt.is_done()
    assert receipt._error is failure
    with pytest.raises(PublishBatchError) as exc_info:
        await batch.wait()
    assert exc_info.value.failures[0] is failure
    assert callbacks == [(9, failure)]


def test_engine_config_update_rolls_back_type_errors() -> None:
    config = EngineConfig(keepalive=60)

    with pytest.raises(TypeError):
        config.update(keepalive=None)
    assert config.keepalive == 60


def test_engine_config_update_is_atomic_across_multiple_fields() -> None:
    config = EngineConfig(
        keepalive=60,
        max_pending_outbound_bytes=1024,
    )

    with pytest.raises(ValueError):
        config.update(
            keepalive=30,
            max_pending_outbound_bytes=-1,
        )

    assert config.keepalive == 60
    assert config.max_pending_outbound_bytes == 1024


def test_attached_config_rejects_changes_with_derived_engine_state() -> None:
    engine = ProtocolEngine()
    original_protocol = engine.config.protocol
    with pytest.raises(AttributeError, match="new ProtocolEngine"):
        engine.config.update(protocol=MQTTProtocolVersion.MQTTv5)
    assert engine.config.protocol is original_protocol


@pytest.mark.parametrize("keepalive", [-1, 65_536])
def test_paho_connect_validates_keepalive_before_starting_the_loop(keepalive: int) -> None:
    client = Client()

    with pytest.raises(ValueError):
        client.connect("unused", keepalive=keepalive)

    assert client._loop is None
    assert client._async._engine.config.keepalive == 60


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


def test_paged_store_protocol_is_exported_from_the_public_package() -> None:
    assert isinstance(MemoryInflightStore(), PagedInflightStore)
