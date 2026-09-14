"""Dispatch invariants for synchronous native message callbacks."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api._delivery import MessageRoute
from mqttium.api.async_client import AsyncClient
from mqttium.types import Message
from tests.support import accept_message


async def _deliver(client: AsyncClient, topic: str, target=None) -> None:
    await accept_message(
        client._delivery,
        Message(topic=topic, payload=b"x"),
        client._message_callback if target is None else target,
    )


def test_inactive_filters_keep_direct_on_message_pointer() -> None:
    client = AsyncClient(message_delivery="callback")

    def fallback(_message):
        pass

    client.on_message = fallback
    assert client._topic_callbacks is None
    assert client._message_callback is fallback
    client.message_callback_add("sensors/#", lambda _message: None)
    assert isinstance(client._message_callback, MessageRoute)
    client.message_callback_remove("sensors/#")
    assert client._topic_callbacks is None
    assert client._message_callback is fallback


def test_on_message_assignment_keeps_route_selection() -> None:
    client = AsyncClient(message_delivery="callback")
    client.message_callback_add("sensors/#", lambda _message: None)

    def fallback(_message):
        pass

    client.on_message = fallback
    assert client.on_message is fallback
    assert isinstance(client._message_callback, MessageRoute)
    client.message_callback_remove("sensors/#")
    assert client._message_callback is fallback


async def test_captured_router_survives_last_filter_removal() -> None:
    client = AsyncClient(message_delivery="callback")
    seen = []
    client.on_message = lambda message: seen.append(f"default:{message.topic}")
    client.message_callback_add("sensors/#", lambda _message: seen.append("filtered"))
    routed = client._message_callback
    client.message_callback_remove("sensors/#")
    await _deliver(client, "sensors/1", routed)
    assert seen == ["default:sensors/1"]


async def test_overlapping_sync_callbacks_run_inline_on_delivering_task() -> None:
    client = AsyncClient(message_delivery="callback")
    seen = []
    delivering = asyncio.current_task()

    def callback(message):
        assert not client._engine_lock.locked()
        assert asyncio.current_task() is delivering
        seen.append(message.topic)

    client.message_callback_add("inline/#", callback)
    client.message_callback_add("inline/+", callback)
    pending = client._delivery.accept(Message("inline/message", b"x"), client._message_callback)
    assert pending is None
    assert seen == ["inline/message", "inline/message"]
    assert client._delivery.callback_invocations == 2
    assert client._delivery.pending_bytes == 0
    assert client._delivery.messages_queue.empty()


@pytest.mark.parametrize("kind", ["error", "cancelled", "awaitable"])
@pytest.mark.parametrize("position", ["first", "later", "fallback"])
async def test_failure_reports_original_callback_and_continues(kind, position) -> None:
    client = AsyncClient(message_delivery="callback")
    seen, errors, returned = [], [], []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))

    async def unintended_work():
        seen.append("must not run")

    def bad(_message):
        seen.append("bad")
        if kind == "error":
            raise RuntimeError("callback failed")
        if kind == "cancelled":
            raise asyncio.CancelledError("user callback cancellation")
        result = unintended_work()
        returned.append(result)
        return result

    def good(_message):
        seen.append("good")

    if position == "first":
        client.message_callback_add("sensors/#", bad)
        client.message_callback_add("sensors/+", good)
    elif position == "later":
        client.message_callback_add("sensors/#", good)
        client.message_callback_add("sensors/+", bad)
        client.message_callback_add("sensors/1", good)
    else:
        client.on_message = bad
        client.message_callback_add("unmatched/#", good)
    try:
        await _deliver(client, "sensors/1")
    finally:
        loop.set_exception_handler(previous)
    assert (
        seen
        == {
            "first": ["bad", "good"],
            "later": ["good", "bad", "good"],
            "fallback": ["bad"],
        }[position]
    )
    assert len(errors) == 1
    assert errors[0]["callback"] is bad
    assert isinstance(
        errors[0]["exception"],
        {
            "error": RuntimeError,
            "cancelled": asyncio.CancelledError,
            "awaitable": TypeError,
        }[kind],
    )
    assert all(coroutine.cr_frame is None for coroutine in returned)


async def test_filter_mutation_before_connection_keeps_current_match_snapshot() -> None:
    client = AsyncClient(message_delivery="callback")
    seen = []

    def first(_message):
        seen.append("first")
        client.message_callback_remove("sensors/+")
        client.message_callback_add("sensors/1", lambda _: seen.append("added"))
        client.message_callback_add("sensors/+", lambda _: seen.append("replaced"))

    client.message_callback_add("sensors/#", first)
    client.message_callback_add("sensors/+", lambda _: seen.append("second"))
    await _deliver(client, "sensors/1")
    assert seen == ["first", "second"]
