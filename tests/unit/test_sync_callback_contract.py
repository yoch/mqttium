from __future__ import annotations

import asyncio
from collections import deque

from mqttium.api import AsyncClient
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message


def _effect(i: int) -> EngineEffect:
    return EngineEffect(
        EffectKind.MESSAGE,
        Message(topic="pair/test", payload=str(i).encode()),
        requires_delivery_mark=False,
    )


async def test_idle_sync_pair_runs_first_inline_without_public_option() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []
    client.on_message = lambda message: seen.append(message.payload.decode())

    applied = client._apply_message_effect_batch_inline(
        deque([_effect(0), _effect(1)]), client._connection_epoch
    )

    assert applied == 2
    assert seen == ["0"]
    assert client._callback_worker_task is not None
    assert client.stats().delivery.callback_queued == 1
    await client._callback_queue.join()
    assert seen == ["0", "1"]
    assert client.stats().delivery.callback_queued == 0
    await client._shutdown_callback_worker(drain=False)


async def test_sync_pair_reserves_tail_and_keeps_reentrant_fifo() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=2)
    seen: list[str] = []

    def callback(message: Message) -> None:
        value = message.payload.decode()
        seen.append(value)
        assert client.stats().delivery.callback_queued >= 1
        if value == "0":
            assert client._delivery.try_enqueue_callback(lambda: seen.append("reentrant"))
            assert client.stats().delivery.callback_queued == 2
            assert not client._delivery.try_enqueue_callback(lambda: seen.append("overflow"))

    client.on_message = callback
    assert (
        client._apply_message_effect_batch_inline(
            deque([_effect(0), _effect(1)]), client._connection_epoch
        )
        == 2
    )
    assert seen == ["0"]
    assert client._callback_queue.maxsize == 2
    await client._callback_queue.join()
    assert seen == ["0", "1", "reentrant"]
    assert client.stats().delivery.callback_queued == 0
    await client._shutdown_callback_worker(drain=False)


async def test_sync_pair_keeps_captured_callback_for_tail() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []

    def replacement(message: Message) -> None:
        seen.append(f"new:{message.payload.decode()}")

    def original(message: Message) -> None:
        value = message.payload.decode()
        seen.append(f"old:{value}")
        if value == "0":
            client.on_message = replacement

    client.on_message = original
    assert (
        client._apply_message_effect_batch_inline(
            deque([_effect(0), _effect(1)]), client._connection_epoch
        )
        == 2
    )
    assert seen == ["old:0"]
    await client._callback_queue.join()
    assert seen == ["old:0", "old:1"]
    await client._shutdown_callback_worker(drain=False)


async def test_larger_sync_burst_runs_only_first_inline() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []
    client.on_message = lambda message: seen.append(message.payload.decode())

    assert (
        client._apply_message_effect_batch_inline(
            deque([_effect(0), _effect(1), _effect(2)]), client._connection_epoch
        )
        == 3
    )
    assert seen == ["0"]
    await client._callback_queue.join()
    assert seen == ["0", "1", "2"]
    await client._shutdown_callback_worker(drain=False)


async def test_async_pair_keeps_worker_path() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []

    async def callback(message: Message) -> None:
        await asyncio.sleep(0)
        seen.append(message.payload.decode())

    client.on_message = callback
    assert (
        client._apply_message_effect_batch_inline(
            deque([_effect(0), _effect(1)]), client._connection_epoch
        )
        == 2
    )
    assert seen == []
    await client._callback_queue.join()
    assert seen == ["0", "1"]
    await client._shutdown_callback_worker(drain=False)


async def test_both_mode_pair_keeps_iterator_and_worker_path() -> None:
    client = AsyncClient(message_delivery="both", max_pending_messages=4)
    seen: list[str] = []
    client.on_message = lambda message: seen.append(message.payload.decode())

    assert (
        client._apply_message_effect_batch_inline(
            deque([_effect(0), _effect(1)]), client._connection_epoch
        )
        == 2
    )
    assert seen == []
    assert client._delivery.messages_queue.qsize() == 2
    await client._callback_queue.join()
    assert seen == ["0", "1"]
    await client._shutdown_callback_worker(drain=False)


async def test_sync_pair_isolates_exception_and_continues() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []
    errors: list[BaseException] = []

    def callback(message: Message) -> None:
        value = message.payload.decode()
        seen.append(value)
        if value == "0":
            raise RuntimeError("boom")

    client.on_message = callback
    client._delivery.report_callback_error = (  # type: ignore[method-assign]
        lambda _callback, exc: errors.append(exc)
    )
    assert (
        client._apply_message_effect_batch_inline(
            deque([_effect(0), _effect(1)]), client._connection_epoch
        )
        == 2
    )
    assert seen == ["0"]
    await client._callback_queue.join()
    assert seen == ["0", "1"]
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    await client._shutdown_callback_worker(drain=False)


async def test_sync_returning_coroutine_is_rejected_inline_and_closed() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []
    errors: list[BaseException] = []

    async def continuation() -> None:
        seen.append("awaited")

    def callback(message: Message):  # type: ignore[no-untyped-def]
        seen.append(f"call:{message.payload.decode()}")
        if message.payload == b"0":
            return continuation()
        return None

    client.on_message = callback
    client._delivery.report_callback_error = (  # type: ignore[method-assign]
        lambda _callback, exc: errors.append(exc)
    )
    assert (
        client._apply_message_effect_batch_inline(
            deque([_effect(0), _effect(1)]), client._connection_epoch
        )
        == 2
    )
    await asyncio.sleep(0)

    assert seen == ["call:0", "call:1"]
    assert len(errors) == 1
    assert isinstance(errors[0], TypeError)
    assert "async def" in str(errors[0])
    assert client._callback_worker_task is not None
    await client._shutdown_callback_worker(drain=False)


async def test_sync_returning_coroutine_is_rejected_on_worker_too() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []
    errors: list[BaseException] = []

    async def continuation() -> None:
        seen.append("awaited")

    def callback():  # type: ignore[no-untyped-def]
        seen.append("called")
        return continuation()

    client._delivery.report_callback_error = (  # type: ignore[method-assign]
        lambda _callback, exc: errors.append(exc)
    )
    client._delivery.spawn_callback(callback)
    await client._callback_queue.join()
    await asyncio.sleep(0)

    assert seen == ["called"]
    assert len(errors) == 1
    assert isinstance(errors[0], TypeError)
    await client._shutdown_callback_worker(drain=False)


async def test_sync_returning_future_is_rejected_without_taking_ownership() -> None:
    client = AsyncClient(message_delivery="callback")
    errors: list[BaseException] = []
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    def callback(_message: Message):  # type: ignore[no-untyped-def]
        return future

    client.on_message = callback
    client._delivery.report_callback_error = (  # type: ignore[method-assign]
        lambda _callback, exc: errors.append(exc)
    )
    assert (
        client._apply_message_effect_batch_inline(
            deque([_effect(0), _effect(1)]), client._connection_epoch
        )
        == 2
    )

    assert len(errors) == 1
    await client._callback_queue.join()
    assert len(errors) == 2
    assert all(isinstance(error, TypeError) for error in errors)
    assert not future.done()
    future.cancel()
    await client._shutdown_callback_worker(drain=False)


async def test_real_task_cancellation_restores_pair_bound() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=2)
    seen: list[str] = []

    async def run_delivery() -> None:
        def callback(message: Message) -> None:
            value = message.payload.decode()
            seen.append(value)
            if value == "0":
                task = asyncio.current_task()
                assert task is not None
                task.cancel()
                raise asyncio.CancelledError

        client.on_message = callback
        client._apply_message_effect_batch_inline(
            deque([_effect(0), _effect(1)]), client._connection_epoch
        )

    task = asyncio.create_task(run_delivery())
    try:
        await task
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError("real task cancellation did not propagate")

    assert seen == ["0"]
    assert client.stats().delivery.callback_queued == 0
    assert client._callback_queue.maxsize == 2
    assert client._callback_worker_task is not None
    await client._shutdown_callback_worker(drain=False)
