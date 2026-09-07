from __future__ import annotations

import asyncio
from collections import deque

import pytest

from mqttium.api import AsyncClient
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message


def _effect(i: int) -> EngineEffect:
    return EngineEffect(
        EffectKind.MESSAGE,
        Message(topic="burst/test", payload=str(i).encode()),
        requires_delivery_mark=False,
    )


async def test_default_batch2_keeps_worker_path() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []
    client.on_message = lambda message: seen.append(message.payload.decode())

    applied = client._apply_message_effect_batch_inline(
        deque([_effect(0), _effect(1)]), client._connection_epoch
    )

    assert applied == 2
    assert seen == []
    assert client._callback_worker_task is not None
    await client._callback_queue.join()
    assert seen == ["0", "1"]
    await client._shutdown_callback_worker(drain=False)


async def test_opt_in_batch2_runs_strict_sync_callbacks_inline() -> None:
    client = AsyncClient(message_delivery="callback", inline_callback_burst=2)
    seen: list[str] = []
    client.on_message = lambda message: seen.append(message.payload.decode())

    applied = client._apply_message_effect_batch_inline(
        deque([_effect(0), _effect(1)]), client._connection_epoch
    )

    assert applied == 2
    assert seen == ["0", "1"]
    assert client._callback_worker_task is None
    assert client.stats().delivery.callback_queued == 0


async def test_opt_in_batch2_reserves_tail_and_keeps_reentrant_fifo() -> None:
    client = AsyncClient(
        message_delivery="callback",
        max_pending_callbacks=2,
        inline_callback_burst=2,
    )
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
    applied = client._apply_message_effect_batch_inline(
        deque([_effect(0), _effect(1)]), client._connection_epoch
    )

    assert applied == 2
    assert seen == ["0", "1"]
    assert client._callback_queue.maxsize == 2
    await client._callback_queue.join()
    assert seen == ["0", "1", "reentrant"]
    assert client.stats().delivery.callback_queued == 0
    await client._shutdown_callback_worker(drain=False)


async def test_opt_in_batch2_keeps_captured_callback_for_tail() -> None:
    client = AsyncClient(message_delivery="callback", inline_callback_burst=2)
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
    assert seen == ["old:0", "old:1"]


async def test_opt_in_rejects_sync_callable_returning_awaitable() -> None:
    client = AsyncClient(message_delivery="callback", inline_callback_burst=2)
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
    assert "strictly synchronous" in str(errors[0])
    assert client._callback_worker_task is None


async def test_opt_in_async_callback_still_uses_worker() -> None:
    client = AsyncClient(message_delivery="callback", inline_callback_burst=2)
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


def test_inline_callback_burst_validation() -> None:
    for value in (0, 3, -1):
        with pytest.raises(ValueError, match="inline_callback_burst must be 1 or 2"):
            AsyncClient(inline_callback_burst=value)  # type: ignore[arg-type]


async def test_opt_in_batch2_isolates_sync_exception_and_continues() -> None:
    client = AsyncClient(message_delivery="callback", inline_callback_burst=2)
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
    assert seen == ["0", "1"]
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert client.stats().delivery.callback_queued == 0


async def test_opt_in_batch2_reports_callback_self_cancellation_and_continues() -> None:
    client = AsyncClient(message_delivery="callback", inline_callback_burst=2)
    seen: list[str] = []
    errors: list[BaseException] = []

    def callback(message: Message) -> None:
        value = message.payload.decode()
        seen.append(value)
        if value == "0":
            raise asyncio.CancelledError("self-cancel")

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
    assert seen == ["0", "1"]
    assert len(errors) == 1
    assert isinstance(errors[0], asyncio.CancelledError)
    assert client.stats().delivery.callback_queued == 0


async def test_opt_in_batch2_rejects_non_coroutine_awaitable_without_owning_it() -> None:
    client = AsyncClient(message_delivery="callback", inline_callback_burst=2)
    seen: list[str] = []
    errors: list[BaseException] = []
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    def callback(message: Message):  # type: ignore[no-untyped-def]
        seen.append(message.payload.decode())
        if message.payload == b"0":
            return future
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
    assert seen == ["0", "1"]
    assert len(errors) == 1
    assert isinstance(errors[0], TypeError)
    assert not future.done()
    future.cancel()


async def test_opt_in_batch2_propagates_real_task_cancellation_and_restores_bound() -> None:
    client = AsyncClient(
        message_delivery="callback",
        max_pending_callbacks=2,
        inline_callback_burst=2,
    )
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
    with pytest.raises(asyncio.CancelledError):
        await task

    assert seen == ["0"]
    assert client.stats().delivery.callback_queued == 0
    assert client._callback_queue.maxsize == 2
    assert client._callback_worker_task is None
