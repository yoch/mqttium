"""Native topic-filtered message callback tests."""

from __future__ import annotations

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.errors import ProtocolError
from mqttium.types import Message


async def _deliver(client: AsyncClient, topic: str) -> None:
    callback = client._message_callback
    assert callback is not None
    await client._invoke(callback, Message(topic=topic, payload=b"payload"))


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
