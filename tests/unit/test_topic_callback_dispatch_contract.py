"""Dispatch invariants for native topic-filtered message callbacks."""

from __future__ import annotations

import asyncio

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
