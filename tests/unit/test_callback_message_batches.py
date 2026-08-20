from __future__ import annotations

from collections import deque

from mqttium.api import AsyncClient
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message


def _effect(i: int) -> EngineEffect:
    return EngineEffect(
        EffectKind.MESSAGE,
        Message(topic="batch/test", payload=str(i).encode()),
        requires_delivery_mark=False,
    )


async def test_message_callback_batch_is_one_physical_job_with_logical_stats() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=8)
    seen: list[bytes] = []
    client.on_message = lambda message: seen.append(message.payload)

    applied = client._apply_message_effect_batch_inline(
        deque(_effect(i) for i in range(4)), client._connection_epoch
    )

    assert applied == 4
    assert len(client._callback_queue._queue) == 1  # type: ignore[attr-defined]
    assert client.stats().delivery.callback_queued == 4
    await client._callback_queue.join()
    assert seen == [b"0", b"1", b"2", b"3"]
    assert client.stats().delivery.callback_queued == 0
    await client._shutdown_callback_worker(drain=False)


async def test_message_batch_respects_logical_callback_limit() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=3)
    client.on_message = lambda _message: None

    applied = client._apply_message_effect_batch_inline(
        deque(_effect(i) for i in range(5)), client._connection_epoch
    )

    assert applied == 3
    assert client.stats().delivery.callback_queued == 3
    assert client._callback_queue.full()
    await client._callback_queue.join()
    assert client.stats().delivery.callback_queued == 0
    assert client._callback_queue.maxsize == 3
    await client._shutdown_callback_worker(drain=False)


async def test_generic_callback_and_message_batch_keep_global_order() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=4)
    seen: list[str] = []
    client.on_message = lambda message: seen.append(message.payload.decode())
    client._callback_queue.put_nowait((lambda: seen.append("generic"), (), None))

    applied = client._apply_message_effect_batch_inline(
        deque(_effect(i) for i in range(4)), client._connection_epoch
    )

    assert applied == 3
    assert client.stats().delivery.callback_queued == 4
    await client._callback_queue.join()
    assert seen == ["generic", "0", "1", "2"]
    await client._shutdown_callback_worker(drain=False)


async def test_batch_callback_error_does_not_skip_following_messages() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=8)
    seen: list[bytes] = []
    errors: list[BaseException] = []

    def callback(message: Message) -> None:
        if message.payload == b"1":
            raise ValueError("boom")
        seen.append(message.payload)

    client.on_message = callback
    client._delivery.report_callback_error = (  # type: ignore[method-assign]
        lambda _callback, exc: errors.append(exc)
    )

    assert (
        client._apply_message_effect_batch_inline(
            deque(_effect(i) for i in range(4)), client._connection_epoch
        )
        == 4
    )
    await client._callback_queue.join()

    assert seen == [b"0", b"2", b"3"]
    assert len(errors) == 1
    assert isinstance(errors[0], ValueError)
    await client._shutdown_callback_worker(drain=False)


async def test_shutdown_releases_reserved_batch_capacity() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=8)
    client.on_message = lambda _message: None

    assert (
        client._apply_message_effect_batch_inline(
            deque(_effect(i) for i in range(4)), client._connection_epoch
        )
        == 4
    )
    assert client._callback_queue.maxsize == 5

    await client._shutdown_callback_worker(drain=False)

    assert client._callback_queue.empty()
    assert client._callback_queue.maxsize == 8
    assert client.stats().delivery.callback_queued == 0


async def test_active_async_batch_keeps_remaining_callbacks_reserved() -> None:
    import asyncio

    client = AsyncClient(message_delivery="callback", max_pending_callbacks=3)
    entered = asyncio.Event()
    release = asyncio.Event()
    seen: list[str] = []

    async def callback(message: Message) -> None:
        seen.append(message.payload.decode())
        if message.payload == b"0":
            entered.set()
            await release.wait()

    client.on_message = callback
    assert (
        client._apply_message_effect_batch_inline(
            deque(_effect(i) for i in range(3)), client._connection_epoch
        )
        == 3
    )
    await entered.wait()

    # Removing the physical batch from asyncio.Queue frees one slot, exactly as
    # dequeuing the first of three ordinary callback jobs would. The other two
    # callbacks remain logically queued while the first async callback is active.
    assert client.stats().delivery.callback_queued == 2
    assert client._delivery.try_enqueue_callback(lambda: seen.append("generic"))
    assert client.stats().delivery.callback_queued == 3
    assert not client._delivery.try_enqueue_callback(lambda: None)

    release.set()
    await client._callback_queue.join()
    assert seen == ["0", "1", "2", "generic"]
    assert client.stats().delivery.callback_queued == 0
    assert client._callback_queue.maxsize == 3
    await client._shutdown_callback_worker(drain=False)
