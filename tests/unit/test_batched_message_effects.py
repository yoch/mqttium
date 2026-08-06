from __future__ import annotations

from collections import deque

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import QoS
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.types import Message


def _effect(*, qos: QoS = QoS.AT_MOST_ONCE, mid: int | None = None) -> EngineEffect:
    return EngineEffect(
        kind=EffectKind.MESSAGE,
        data=Message(topic="hot/path", payload=b"payload", qos=qos, mid=mid),
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


@pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
def test_batch_defers_acknowledged_messages(qos: QoS) -> None:
    client = AsyncClient(message_delivery="callback")
    client.on_message = lambda _message: None

    applied = client._apply_message_effect_batch_inline(
        deque([_effect(qos=qos, mid=7), _effect()]), client._connection_epoch
    )

    assert applied == 0
    assert client._callback_queue.empty()


def test_batch_stops_at_first_non_qos0_effect() -> None:
    client = AsyncClient(message_delivery="iterator")
    effects = deque(
        [
            _effect(),
            _effect(qos=QoS.AT_LEAST_ONCE, mid=7),
            _effect(),
        ]
    )

    applied = client._apply_message_effect_batch_inline(effects, client._connection_epoch)

    assert applied == 1
    assert client._messages.qsize() == 1
    assert client._message_ready.is_set()
