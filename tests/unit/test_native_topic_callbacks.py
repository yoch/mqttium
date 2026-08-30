"""Native topic-filtered message callback tests."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.errors import ProtocolError
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.types import Message


async def _deliver(client: AsyncClient, topic: str) -> None:
    callback = client._message_callback
    assert callback is not None
    await client._invoke(callback, Message(topic=topic, payload=b"payload"))


async def _apply(client: AsyncClient, topic: str) -> None:
    await client._apply_effect(
        EngineEffect(
            kind=EffectKind.MESSAGE,
            data=Message(topic=topic, payload=b"payload"),
        ),
        nowait=False,
    )


async def test_topic_callbacks_override_default_only_when_a_filter_matches() -> None:
    client = AsyncClient(message_delivery="callback")
    calls: list[str] = []

    client.on_message = lambda message: calls.append(f"default:{message.topic}")
    client.add_message_callback("sensors/+/temp", lambda _message: calls.append("plus"))
    client.add_message_callback("sensors/kitchen/#", lambda _message: calls.append("hash"))

    await _deliver(client, "sensors/kitchen/temp")
    assert calls == ["plus", "hash"]

    calls.clear()
    await _deliver(client, "other/topic")
    assert calls == ["default:other/topic"]


async def test_topic_callbacks_preserve_order_across_async_callbacks() -> None:
    client = AsyncClient(message_delivery="callback")
    calls: list[str] = []

    def first(_message: Message) -> None:
        calls.append("first")

    async def second(_message: Message) -> None:
        calls.append("second")

    def third(_message: Message) -> None:
        calls.append("third")

    client.add_message_callback("sensors/#", first)
    client.add_message_callback("sensors/+/temp", second)
    client.add_message_callback("sensors/kitchen/temp", third)

    await _deliver(client, "sensors/kitchen/temp")
    assert calls == ["first", "second", "third"]


async def test_matching_callback_failure_does_not_suppress_later_matches() -> None:
    client = AsyncClient(message_delivery="callback")
    calls: list[str] = []
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    errors: list[dict[str, object]] = []
    loop.set_exception_handler(lambda _loop, context: errors.append(context))

    def broken(_message: Message) -> None:
        calls.append("broken")
        raise RuntimeError("boom")

    try:
        client.add_message_callback("sensors/#", broken)
        client.add_message_callback("sensors/+/temp", lambda _message: calls.append("after"))
        await _deliver(client, "sensors/kitchen/temp")
    finally:
        loop.set_exception_handler(previous_handler)

    assert calls == ["broken", "after"]
    assert len(errors) == 1
    assert isinstance(errors[0].get("exception"), RuntimeError)


async def test_all_sync_overlapping_callbacks_stay_inline() -> None:
    client = AsyncClient(message_delivery="callback")
    calls: list[str] = []
    client.add_message_callback("sensors/#", lambda _message: calls.append("hash"))
    client.add_message_callback("sensors/+/temp", lambda _message: calls.append("plus"))

    callback = client._message_callback
    assert callback is not None
    assert client._can_dispatch_callback_inline(callback)
    client._dispatch_callback_inline(
        callback,
        Message(topic="sensors/kitchen/temp", payload=b"payload"),
    )

    assert calls == ["hash", "plus"]
    assert client._callback_worker_task is None


async def test_topic_callback_selects_auto_callback_delivery() -> None:
    client = AsyncClient()
    seen: list[str] = []
    client.add_message_callback("sensors/+", lambda message: seen.append(message.topic))

    await _apply(client, "sensors/1")
    await asyncio.wait_for(client._callback_queue.join(), timeout=1.0)

    assert seen == ["sensors/1"]
    assert client._messages.empty()
    await client._shutdown_callback_worker(drain=False)


async def test_iterator_mode_ignores_topic_callbacks() -> None:
    client = AsyncClient(message_delivery="iterator")
    seen: list[str] = []
    client.add_message_callback("sensors/+", lambda message: seen.append(message.topic))

    await _apply(client, "sensors/1")

    assert seen == []
    assert client._messages.get_nowait().topic == "sensors/1"


async def test_both_mode_delivers_to_topic_callback_and_iterator() -> None:
    client = AsyncClient(message_delivery="both")
    seen: list[str] = []
    client.add_message_callback("sensors/+", lambda message: seen.append(message.topic))

    await _apply(client, "sensors/1")
    await asyncio.wait_for(client._callback_queue.join(), timeout=1.0)

    assert seen == ["sensors/1"]
    assert client._messages.get_nowait().topic == "sensors/1"
    await client._shutdown_callback_worker(drain=False)


async def test_replacing_a_topic_callback_keeps_one_registration() -> None:
    client = AsyncClient(message_delivery="callback")
    calls: list[str] = []

    client.add_message_callback("sensors/#", lambda _message: calls.append("old"))
    client.add_message_callback("sensors/#", lambda _message: calls.append("new"))

    await _deliver(client, "sensors/temp")
    assert calls == ["new"]


def test_last_removal_restores_direct_on_message_hot_path() -> None:
    client = AsyncClient(message_delivery="callback")

    def fallback(_message: Message) -> None:
        pass

    client.on_message = fallback
    assert client._topic_callbacks is None
    assert client._message_callback is fallback

    client.add_message_callback("sensors/#", lambda _message: None)
    assert client._topic_callbacks is not None
    assert client._message_callback is not fallback

    client.remove_message_callback("sensors/#")
    assert client._topic_callbacks is None
    assert client._message_callback is fallback


def test_on_message_assignment_updates_fallback_while_filters_are_active() -> None:
    client = AsyncClient(message_delivery="callback")
    client.add_message_callback("sensors/#", lambda _message: None)
    routed = client._message_callback

    def fallback(_message: Message) -> None:
        pass

    client.on_message = fallback
    assert client.on_message is fallback
    assert client._message_callback is routed

    client.remove_message_callback("sensors/#")
    assert client._message_callback is fallback


def test_remove_unknown_topic_callback_is_a_noop() -> None:
    client = AsyncClient(message_delivery="callback")
    client.remove_message_callback("missing/#")
    assert client._topic_callbacks is None


def test_topic_callback_filter_is_validated() -> None:
    client = AsyncClient(message_delivery="callback")
    with pytest.raises(ProtocolError):
        client.add_message_callback("bad/#/filter", lambda _message: None)
