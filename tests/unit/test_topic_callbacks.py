"""Native topic-filtered message callbacks on AsyncClient."""

from __future__ import annotations

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.errors import ProtocolError
from mqttium.protocol.engine import EffectKind
from mqttium.types import Message
from tests.support import deliver_message


async def _deliver(
    client: AsyncClient, topic: str = "delivery/test", payload: bytes = b"x"
) -> None:
    await deliver_message(client, Message(topic=topic, payload=payload))


async def test_topic_callback_takes_precedence_over_on_message() -> None:
    client = AsyncClient(client_id="topic-precedence", message_delivery="callback")
    matched: list[str] = []
    defaults: list[str] = []
    client.message_callback_add("sensors/+", lambda message: matched.append(message.topic))
    client.on_message = lambda message: defaults.append(message.topic)

    await _deliver(client, "sensors/1")
    await _deliver(client, "other")

    assert matched == ["sensors/1"]
    assert defaults == ["other"]
    assert client._delivery.callback_invocations == 2


async def test_topic_callback_uses_explicit_callback_delivery() -> None:
    client = AsyncClient(client_id="topic-auto", message_delivery="callback")
    seen: list[str] = []
    client.message_callback_add("sensors/+", lambda message: seen.append(message.topic))

    await _deliver(client, "sensors/1")

    assert seen == ["sensors/1"]
    assert client._delivery.messages_queue.empty()


async def test_unmatched_topic_callback_does_not_fill_iterator() -> None:
    client = AsyncClient(client_id="topic-unmatched-auto", message_delivery="callback")
    seen: list[str] = []
    client.message_callback_add("sensors/+", lambda message: seen.append(message.topic))

    await _deliver(client, "other")

    assert seen == []
    assert client._delivery.callback_invocations == 0
    assert client._delivery.messages_queue.empty()


async def test_overlapping_filters_run_in_registration_order() -> None:
    client = AsyncClient(client_id="topic-overlap", message_delivery="callback")
    seen: list[str] = []
    client.message_callback_add("sensors/#", lambda _message: seen.append("hash"))
    client.message_callback_add("sensors/kitchen/temp", lambda _message: seen.append("exact"))
    client.message_callback_add("sensors/+/temp", lambda _message: seen.append("plus"))

    await _deliver(client, "sensors/kitchen/temp")

    assert seen == ["hash", "exact", "plus"]
    assert client._delivery.callback_invocations == 3


async def test_replace_keeps_filter_order() -> None:
    client = AsyncClient(client_id="topic-replace", message_delivery="callback")
    seen: list[str] = []
    client.message_callback_add("sensors/#", lambda _message: seen.append("hash"))
    client.message_callback_add("sensors/temp", lambda _message: seen.append("old"))
    client.message_callback_add("sensors/temp", lambda _message: seen.append("new"))

    await _deliver(client, "sensors/temp")

    assert seen == ["hash", "new"]


async def test_remove_restores_on_message_and_clears_matcher() -> None:
    client = AsyncClient(client_id="topic-remove", message_delivery="callback")
    seen: list[str] = []
    client.message_callback_add("sensors/+", lambda message: seen.append(f"filter:{message.topic}"))
    client.on_message = lambda message: seen.append(f"default:{message.topic}")

    client.message_callback_remove("sensors/+")
    assert client._topic_callbacks is None

    await _deliver(client, "sensors/1")

    assert seen == ["default:sensors/1"]


async def test_remove_unknown_filter_is_a_no_op() -> None:
    client = AsyncClient(client_id="topic-remove-missing", message_delivery="callback")
    client.message_callback_remove("sensors/+")
    client.message_callback_add("sensors/+", lambda _message: None)
    client.message_callback_add("other/#", lambda _message: None)
    client.message_callback_remove("missing/#")
    assert client._topic_callbacks is not None
    client.message_callback_remove("other/#")
    assert client._topic_callbacks is not None
    client.message_callback_remove("sensors/+")
    assert client._topic_callbacks is None


def test_invalid_filter_is_rejected_before_registration() -> None:
    client = AsyncClient(client_id="topic-invalid", message_delivery="callback")
    with pytest.raises(ProtocolError):
        client.message_callback_add("sport/#/ranking", lambda _message: None)
    assert client._topic_callbacks is None


async def test_shared_subscription_filter_matches_literally() -> None:
    client = AsyncClient(client_id="topic-shared", message_delivery="callback")
    seen: list[str] = []
    client.message_callback_add(
        "$share/group/sensors/#",
        lambda _message: seen.append("shared"),
    )
    client.message_callback_add("sensors/#", lambda _message: seen.append("normal"))

    await _deliver(client, "sensors/temp")

    assert seen == ["normal"]


async def test_iterator_mode_ignores_topic_callbacks() -> None:
    client = AsyncClient(client_id="topic-iterator", message_delivery="iterator")
    seen: list[str] = []
    client.message_callback_add("sensors/+", lambda message: seen.append(message.topic))

    await _deliver(client, "sensors/1")

    assert seen == []
    assert client._delivery.callback_invocations == 0
    assert (await anext(client.messages())).topic == "sensors/1"


async def test_async_topic_callback_is_rejected_before_route_mutation() -> None:
    client = AsyncClient(client_id="topic-async", message_delivery="callback")
    seen: list[str] = []

    async def on_sensor(message: Message) -> None:
        seen.append(message.topic)

    with pytest.raises(TypeError, match=r"synchronous; use messages\(\)"):
        client.message_callback_add("sensors/+", on_sensor)
    assert client._topic_callbacks is None
    assert seen == []


async def test_sync_topic_callback_runs_on_reader_outside_engine_lock() -> None:
    client = AsyncClient(client_id="topic-inline", message_delivery="callback")
    seen: list[tuple[str, bool]] = []
    client.message_callback_add(
        "inline/#",
        lambda message: seen.append((message.topic, client._engine_lock.locked())),
    )

    async with client._engine_lock:
        client._engine._emit(
            EffectKind.MESSAGE,
            Message(topic="inline/message", payload=b"x"),
        )
        client._effect_pump.collect_from_engine()
        assert seen == []

    client._effect_pump.drain_inline()
    assert seen == []
    await client._effect_pump.drain()
    await client._delivery_lane.drain()
    assert seen == [("inline/message", False)]
    assert client._delivery.callback_invocations == 1


async def test_async_replacement_preserves_existing_sync_route() -> None:
    client = AsyncClient(client_id="topic-overlap-async", message_delivery="callback")
    seen: list[str] = []

    def on_hash(message: Message) -> None:
        seen.append(f"hash:{message.topic}")

    async def on_plus(message: Message) -> None:
        seen.append(f"plus:{message.topic}")

    client.message_callback_add("sensors/#", on_hash)
    with pytest.raises(TypeError, match="synchronous"):
        client.message_callback_add("sensors/#", on_plus)
    await _deliver(client, "sensors/1")

    assert seen == ["hash:sensors/1"]
