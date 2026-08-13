from __future__ import annotations

from collections import deque

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import InboundQoSState, QoS
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.types import InboundMessage, Message


def _effect(
    *,
    qos: QoS = QoS.AT_MOST_ONCE,
    mid: int | None = None,
    requires_delivery_mark: bool = False,
) -> EngineEffect:
    return EngineEffect(
        kind=EffectKind.MESSAGE,
        data=Message(topic="hot/path", payload=b"payload", qos=qos, mid=mid),
        requires_delivery_mark=requires_delivery_mark,
    )


@pytest.mark.asyncio
async def test_small_qos0_callback_messages_apply_as_one_synchronous_batch() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[Message] = []
    client.on_message = seen.append
    effects = deque([_effect(), _effect(), _effect()])

    applied = client._apply_message_effect_batch_inline(effects, client._connection_epoch)

    assert applied == 3
    await client._callback_queue.join()
    assert len(seen) == 3
    await client._shutdown_callback_worker(drain=False)


@pytest.mark.asyncio
async def test_small_auto_qos1_callback_messages_share_the_batch() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[Message] = []
    client.on_message = seen.append
    effects = deque(
        [
            _effect(qos=QoS.AT_LEAST_ONCE, mid=1),
            _effect(qos=QoS.AT_LEAST_ONCE, mid=2),
            _effect(qos=QoS.AT_LEAST_ONCE, mid=3),
        ]
    )

    applied = client._apply_message_effect_batch_inline(effects, client._connection_epoch)

    assert applied == 3
    await client._callback_queue.join()
    assert len(seen) == 3
    await client._shutdown_callback_worker(drain=False)


def test_single_message_keeps_the_established_effect_path() -> None:
    client = AsyncClient(message_delivery="iterator")

    applied = client._apply_message_effect_batch_inline(
        deque([_effect()]), client._connection_epoch
    )

    assert applied == 0
    assert client._messages.empty()


def test_stale_epoch_keeps_the_established_effect_path() -> None:
    client = AsyncClient(message_delivery="iterator")

    applied = client._apply_message_effect_batch_inline(
        deque([_effect(), _effect()]), client._connection_epoch - 1
    )

    assert applied == 0
    assert client._messages.empty()


def test_batch_stops_before_half_delivering_both_mode() -> None:
    client = AsyncClient(
        message_delivery="both",
        max_pending_callbacks=1,
        max_pending_messages=1,
    )
    client.on_message = lambda _message: None
    sentinel = (lambda: None, (), None)
    client._callback_queue.put_nowait(sentinel)

    applied = client._apply_message_effect_batch_inline(
        deque([_effect(), _effect()]), client._connection_epoch
    )

    assert applied == 0
    assert client._messages.empty()
    assert client._callback_queue.get_nowait() is sentinel
    client._callback_queue.task_done()


def test_unclassified_identified_message_is_not_batched() -> None:
    client = AsyncClient(message_delivery="iterator")
    unknown = EngineEffect(
        kind=EffectKind.MESSAGE,
        data=Message(
            topic="hot/path",
            payload=b"payload",
            qos=QoS.AT_LEAST_ONCE,
            mid=7,
        ),
    )

    applied = client._apply_message_effect_batch_inline(
        deque([unknown, _effect()]),
        client._connection_epoch,
    )

    assert unknown.requires_delivery_mark is None
    assert applied == 0
    assert client._messages.empty()


@pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
def test_batch_defers_persisted_acknowledged_messages(qos: QoS) -> None:
    client = AsyncClient(message_delivery="callback")
    client.on_message = lambda _message: None

    applied = client._apply_message_effect_batch_inline(
        deque(
            [
                _effect(qos=qos, mid=7, requires_delivery_mark=True),
                _effect(),
            ]
        ),
        client._connection_epoch,
    )

    assert applied == 0
    assert client._callback_queue.empty()


def test_batch_stops_at_first_persisted_message() -> None:
    client = AsyncClient(message_delivery="iterator")
    effects = deque(
        [
            _effect(),
            _effect(qos=QoS.AT_LEAST_ONCE, mid=7, requires_delivery_mark=True),
            _effect(),
        ]
    )

    applied = client._apply_message_effect_batch_inline(effects, client._connection_epoch)

    assert applied == 1
    assert client._messages.qsize() == 1
    assert client._message_ready.is_set()


def test_batch_stops_at_first_non_message_effect() -> None:
    client = AsyncClient(message_delivery="iterator")
    effects = deque(
        [
            _effect(),
            EngineEffect(kind=EffectKind.PINGRESP),
            _effect(),
        ]
    )

    applied = client._apply_message_effect_batch_inline(effects, client._connection_epoch)

    assert applied == 1
    assert client._messages.qsize() == 1


def test_drain_inline_applies_auto_qos1_batch_without_scheduling() -> None:
    client = AsyncClient(message_delivery="iterator")
    pump = client._effect_pump
    pump.pending = deque(
        [
            _effect(qos=QoS.AT_LEAST_ONCE, mid=1),
            _effect(qos=QoS.AT_LEAST_ONCE, mid=2),
        ]
    )
    pump.pending_epoch = client._connection_epoch
    pump.enqueued = 2

    pump.drain_inline()

    assert not pump.pending
    assert pump.task is None
    assert client._messages.qsize() == 2


@pytest.mark.asyncio
async def test_auto_qos1_single_effect_skips_absent_delivery_mark() -> None:
    client = AsyncClient(message_delivery="iterator")
    marked: list[int] = []
    client._engine.mark_inbound_delivered = marked.append  # type: ignore[method-assign]

    await client._apply_effect(
        _effect(qos=QoS.AT_LEAST_ONCE, mid=7),
        nowait=False,
        epoch=client._connection_epoch,
    )

    assert marked == []
    assert client._messages.qsize() == 1


@pytest.mark.asyncio
async def test_replayed_persisted_qos1_marks_even_when_current_mode_is_auto_ack() -> None:
    store = MemoryInflightStore()
    store.put_in(
        InboundMessage(
            mid=7,
            topic="hot/path",
            payload=b"payload",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=InboundQoSState.WAIT_PUBACK,
            delivered=False,
        )
    )
    client = AsyncClient(
        message_delivery="iterator",
        manual_ack=False,
        store=store,
    )

    client._engine.inbound.replay_session()
    effects = client._engine.take_effects()
    message_effect = next(effect for effect in effects if effect.kind is EffectKind.MESSAGE)
    assert message_effect.requires_delivery_mark is True

    await client._apply_effect(
        message_effect,
        nowait=False,
        epoch=client._connection_epoch,
    )

    record = store.get_in(7)
    assert record is not None
    assert record.delivered is True
