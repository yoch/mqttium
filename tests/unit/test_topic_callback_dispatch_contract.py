"""Dispatch invariants for native topic-filtered message callbacks."""

from __future__ import annotations

import asyncio
import inspect

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.types import Message


async def _deliver(client: AsyncClient, topic: str) -> None:
    await client._apply_effect(
        EngineEffect(
            kind=EffectKind.MESSAGE,
            data=Message(topic=topic, payload=b"x"),
        ),
        nowait=False,
    )


def test_inactive_filters_keep_direct_on_message_pointer() -> None:
    client = AsyncClient(message_delivery="callback")

    def fallback(_message: Message) -> None:
        pass

    client.on_message = fallback
    assert client._topic_callbacks is None
    assert client._message_callback is fallback

    client.message_callback_add("sensors/#", lambda _message: None)
    assert client._topic_callbacks is not None
    assert client._message_callback is not fallback

    client.message_callback_remove("sensors/#")
    assert client._topic_callbacks is None
    assert client._message_callback is fallback


def test_on_message_assignment_updates_fallback_without_replacing_router() -> None:
    client = AsyncClient(message_delivery="callback")
    client.message_callback_add("sensors/#", lambda _message: None)
    routed = client._message_callback

    def fallback(_message: Message) -> None:
        pass

    client.on_message = fallback
    assert client.on_message is fallback
    assert client._message_callback is routed

    client.message_callback_remove("sensors/#")
    assert client._message_callback is fallback


async def test_captured_router_survives_last_filter_removal() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []
    client.on_message = lambda message: seen.append(f"default:{message.topic}")
    client.message_callback_add("sensors/#", lambda _message: seen.append("filtered"))
    routed = client._message_callback
    assert routed is not None

    client.message_callback_remove("sensors/#")
    assert client._topic_callbacks is None
    await client._invoke(routed, Message(topic="sensors/1", payload=b"x"))

    assert seen == ["default:sensors/1"]


async def test_overlapping_sync_callbacks_remain_inline() -> None:
    client = AsyncClient(client_id="topic-overlap-inline", message_delivery="callback")
    seen: list[str] = []
    client.message_callback_add("inline/#", lambda _message: seen.append("hash"))
    client.message_callback_add("inline/+", lambda _message: seen.append("plus"))

    async with client._engine_lock:
        client._engine._emit(
            EffectKind.MESSAGE,
            Message(topic="inline/message", payload=b"x"),
        )
        client._collect_effects_locked()
        assert seen == []

    client._drain_effects_inline()
    assert seen == ["hash", "plus"]
    assert client._callback_worker_task is None


async def test_sync_failure_does_not_suppress_later_match() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []
    errors: list[dict[str, object]] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))

    def bad(_message: Message) -> None:
        seen.append("bad")
        raise RuntimeError("boom")

    def good(_message: Message) -> None:
        seen.append("good")

    try:
        client.message_callback_add("sensors/#", bad)
        client.message_callback_add("sensors/+", good)
        callback = client._message_callback
        assert callback is not None
        await client._invoke(callback, Message(topic="sensors/1", payload=b"x"))
    finally:
        loop.set_exception_handler(previous)

    assert seen == ["bad", "good"]
    assert len(errors) == 1
    assert errors[0]["callback"] is bad
    assert isinstance(errors[0]["exception"], RuntimeError)


async def test_async_failure_does_not_suppress_later_match() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []
    errors: list[dict[str, object]] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))

    async def bad(_message: Message) -> None:
        seen.append("bad:start")
        await asyncio.sleep(0)
        raise RuntimeError("boom")

    def good(_message: Message) -> None:
        seen.append("good")

    try:
        client.message_callback_add("sensors/#", bad)
        client.message_callback_add("sensors/+", good)
        callback = client._message_callback
        assert callback is not None
        await client._invoke(callback, Message(topic="sensors/1", payload=b"x"))
    finally:
        loop.set_exception_handler(previous)

    assert seen == ["bad:start", "good"]
    assert len(errors) == 1
    assert errors[0]["callback"] is bad
    assert isinstance(errors[0]["exception"], RuntimeError)


async def test_callback_self_cancellation_is_reported_and_sequence_continues() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []
    errors: list[dict[str, object]] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))

    def cancelling(_message: Message) -> None:
        seen.append("cancel")
        raise asyncio.CancelledError

    def good(_message: Message) -> None:
        seen.append("good")

    try:
        client.message_callback_add("sensors/#", cancelling)
        client.message_callback_add("sensors/+", good)
        callback = client._message_callback
        assert callback is not None
        await client._invoke(callback, Message(topic="sensors/1", payload=b"x"))
    finally:
        loop.set_exception_handler(previous)

    assert seen == ["cancel", "good"]
    assert len(errors) == 1
    assert errors[0]["callback"] is cancelling
    assert isinstance(errors[0]["exception"], asyncio.CancelledError)


async def test_real_task_cancellation_stops_callback_sequence() -> None:
    client = AsyncClient(message_delivery="callback")
    started = asyncio.Event()
    seen: list[str] = []

    async def blocking(_message: Message) -> None:
        started.set()
        await asyncio.sleep(60)

    def later(_message: Message) -> None:
        seen.append("later")

    client.message_callback_add("sensors/#", blocking)
    client.message_callback_add("sensors/+", later)
    callback = client._message_callback
    assert callback is not None
    task = asyncio.create_task(client._invoke(callback, Message(topic="sensors/1", payload=b"x")))
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert seen == []


async def test_auto_returns_to_iterator_after_last_filter_without_fallback() -> None:
    client = AsyncClient(client_id="topic-auto-restore")
    client.message_callback_add("sensors/#", lambda _message: None)
    client.message_callback_remove("sensors/#")

    assert client._topic_callbacks is None
    assert client._message_callback is None
    await _deliver(client, "sensors/1")

    assert client._messages.get_nowait().topic == "sensors/1"


async def test_sync_fallback_failure_is_reported_as_fallback() -> None:
    client = AsyncClient(message_delivery="callback")
    errors: list[dict[str, object]] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))

    def bad(_message: Message) -> None:
        raise RuntimeError("fallback")

    try:
        client.on_message = bad
        client.message_callback_add("sensors/#", lambda _message: None)
        callback = client._message_callback
        assert callback is not None
        await client._invoke(callback, Message(topic="other", payload=b"x"))
    finally:
        loop.set_exception_handler(previous)

    assert len(errors) == 1
    assert errors[0]["callback"] is bad
    assert isinstance(errors[0]["exception"], RuntimeError)


async def test_async_fallback_failure_is_reported_as_fallback() -> None:
    client = AsyncClient(message_delivery="callback")
    errors: list[dict[str, object]] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))

    async def bad(_message: Message) -> None:
        await asyncio.sleep(0)
        raise RuntimeError("fallback")

    try:
        client.on_message = bad
        client.message_callback_add("sensors/#", lambda _message: None)
        callback = client._message_callback
        assert callback is not None
        await client._invoke(callback, Message(topic="other", payload=b"x"))
    finally:
        loop.set_exception_handler(previous)

    assert len(errors) == 1
    assert errors[0]["callback"] is bad
    assert isinstance(errors[0]["exception"], RuntimeError)


async def test_fallback_self_cancellation_is_reported() -> None:
    client = AsyncClient(message_delivery="callback")
    errors: list[dict[str, object]] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))

    def cancelling(_message: Message) -> None:
        raise asyncio.CancelledError

    try:
        client.on_message = cancelling
        client.message_callback_add("sensors/#", lambda _message: None)
        callback = client._message_callback
        assert callback is not None
        await client._invoke(callback, Message(topic="other", payload=b"x"))
    finally:
        loop.set_exception_handler(previous)

    assert len(errors) == 1
    assert errors[0]["callback"] is cancelling
    assert isinstance(errors[0]["exception"], asyncio.CancelledError)


async def test_later_sync_failure_after_async_match_is_isolated() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []
    errors: list[dict[str, object]] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))

    async def first(_message: Message) -> None:
        seen.append("first")
        await asyncio.sleep(0)

    def bad(_message: Message) -> None:
        seen.append("bad")
        raise RuntimeError("later")

    def good(_message: Message) -> None:
        seen.append("good")

    try:
        client.message_callback_add("sensors/#", first)
        client.message_callback_add("sensors/+", bad)
        client.message_callback_add("sensors/1", good)
        callback = client._message_callback
        assert callback is not None
        await client._invoke(callback, Message(topic="sensors/1", payload=b"x"))
    finally:
        loop.set_exception_handler(previous)

    assert seen == ["first", "bad", "good"]
    assert len(errors) == 1
    assert errors[0]["callback"] is bad


async def test_later_async_failure_and_self_cancellation_are_isolated() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []
    errors: list[dict[str, object]] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))

    async def first(_message: Message) -> None:
        seen.append("first")
        await asyncio.sleep(0)

    async def bad(_message: Message) -> None:
        seen.append("bad")
        await asyncio.sleep(0)
        raise RuntimeError("later async")

    def cancelling(_message: Message) -> None:
        seen.append("cancel")
        raise asyncio.CancelledError

    def good(_message: Message) -> None:
        seen.append("good")

    try:
        client.message_callback_add("sensors/#", first)
        client.message_callback_add("sensors/+", bad)
        client.message_callback_add("sensors/1", cancelling)
        client.message_callback_add("+/1", good)
        callback = client._message_callback
        assert callback is not None
        await client._invoke(callback, Message(topic="sensors/1", payload=b"x"))
    finally:
        loop.set_exception_handler(previous)

    assert seen == ["first", "bad", "cancel", "good"]
    assert len(errors) == 2
    assert errors[0]["callback"] is bad
    assert errors[1]["callback"] is cancelling


async def test_reentrant_sync_match_can_fill_queue_before_async_match() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=1)
    seen: list[str] = []
    nested = Message(topic="nested/x", payload=b"nested")

    def first(_message: Message) -> None:
        seen.append("first")
        pending = client._accept_message(nested, client._message_callback)
        assert pending is None

    async def second(_message: Message) -> None:
        seen.append("second")

    client.message_callback_add("outer/#", first)
    client.message_callback_add("outer/+", second)
    client.message_callback_add("nested/#", lambda _message: seen.append("nested"))
    callback = client._message_callback
    assert callback is not None

    client._delivery.dispatch_callback_inline(
        callback,
        Message(topic="outer/x", payload=b"outer"),
    )
    assert client.stats().delivery.callback_queued == 1
    assert client.stats().delivery.callback_limit == 1
    assert client._callback_queue.full()
    assert client._delivery._inline_continuation is not None

    await client._callback_queue.join()
    assert seen == ["first", "nested", "second"]
    assert client.stats().delivery.callback_queued == 0
    assert client._delivery._inline_continuation is None
    await client._shutdown_callback_worker(drain=False)


async def test_inline_continuation_chains_after_reentrant_callback_batch() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=2)
    seen: list[str] = []
    batch = [
        Message(topic="batch/1", payload=b"1"),
        Message(topic="batch/2", payload=b"2"),
    ]

    def batch_callback(message: Message) -> None:
        seen.append(f"batch:{message.payload.decode()}")

    def first(_message: Message) -> None:
        seen.append("first")
        client._delivery._enqueue_message_batch(
            batch_callback,
            batch,
            iterator_delivery=False,
        )

    async def second(_message: Message) -> None:
        seen.append("second")

    client.message_callback_add("outer/#", first)
    client.message_callback_add("outer/+", second)
    callback = client._message_callback
    assert callback is not None

    client._delivery.dispatch_callback_inline(
        callback,
        Message(topic="outer/x", payload=b"outer"),
    )
    assert client.stats().delivery.callback_queued == 2
    assert client.stats().delivery.callback_limit == 2
    assert client._callback_queue.full()
    assert client._delivery._inline_continuation is not None

    await client._callback_queue.join()
    assert seen == ["first", "batch:1", "batch:2", "second"]
    assert client.stats().delivery.callback_queued == 0
    assert client._delivery._callback_batch_reserved == 0
    assert client._delivery._inline_continuation is None
    await client._shutdown_callback_worker(drain=False)


async def test_inline_handoff_reports_continuation_failure_as_original_callback() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=1)
    errors: list[dict[str, object]] = []
    seen: list[str] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))

    def queued() -> None:
        seen.append("queued")

    async def fail_after_handoff() -> None:
        seen.append("continuation")
        raise RuntimeError("handoff")

    def outer() -> object:
        client._delivery.spawn_callback(queued)
        return fail_after_handoff()

    try:
        client._delivery.dispatch_callback_inline(outer)
        assert client._delivery._inline_continuation is not None
        await client._callback_queue.join()
    finally:
        loop.set_exception_handler(previous)
        await client._shutdown_callback_worker(drain=False)

    assert seen == ["queued", "continuation"]
    assert len(errors) == 1
    assert errors[0]["callback"] is outer
    assert isinstance(errors[0]["exception"], RuntimeError)


async def test_inline_continuation_failure_with_queue_space_is_original_callback() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=8)
    errors: list[dict[str, object]] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))

    async def fail_after_handoff() -> None:
        raise RuntimeError("queued-handoff")

    def outer() -> object:
        return fail_after_handoff()

    try:
        client._delivery.dispatch_callback_inline(outer)
        assert client._delivery._inline_continuation is None
        await client._callback_queue.join()
    finally:
        loop.set_exception_handler(previous)
        await client._shutdown_callback_worker(drain=False)

    assert len(errors) == 1
    assert errors[0]["callback"] is outer
    assert isinstance(errors[0]["exception"], RuntimeError)


async def test_shutdown_discards_parked_inline_continuation() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=1)
    started = asyncio.Event()
    block = asyncio.Event()
    seen: list[str] = []

    async def queued() -> None:
        seen.append("queued")
        started.set()
        await block.wait()

    async def pending() -> None:
        seen.append("continuation")

    def outer() -> object:
        client._delivery.spawn_callback(queued)
        return pending()

    client._delivery.dispatch_callback_inline(outer)
    parked = client._delivery._inline_continuation
    assert parked is not None
    await started.wait()
    assert client._delivery._inline_continuation is parked

    await client._shutdown_callback_worker(drain=False)
    assert client._delivery._inline_continuation is None
    assert inspect.getcoroutinestate(parked[1]) == inspect.CORO_CLOSED
    assert seen == ["queued"]


async def test_filter_mutation_during_dispatch_does_not_change_current_matches() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []

    def first(_message: Message) -> None:
        seen.append("first")
        client.message_callback_remove("sensors/+")
        client.message_callback_add("sensors/1", lambda _message: seen.append("added"))
        client.message_callback_add("sensors/+", lambda _message: seen.append("replaced"))

    def second(_message: Message) -> None:
        seen.append("second")

    client.message_callback_add("sensors/#", first)
    client.message_callback_add("sensors/+", second)
    callback = client._message_callback
    assert callback is not None
    await client._invoke(callback, Message(topic="sensors/1", payload=b"x"))

    assert seen == ["first", "second"]


async def test_async_filter_mutation_during_dispatch_keeps_captured_matches() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []

    async def first(_message: Message) -> None:
        seen.append("first")
        await asyncio.sleep(0)
        client.message_callback_remove("sensors/+")
        client.message_callback_add("sensors/1", lambda _message: seen.append("added"))

    def second(_message: Message) -> None:
        seen.append("second")

    client.message_callback_add("sensors/#", first)
    client.message_callback_add("sensors/+", second)
    callback = client._message_callback
    assert callback is not None
    await client._invoke(callback, Message(topic="sensors/1", payload=b"x"))

    assert seen == ["first", "second"]
