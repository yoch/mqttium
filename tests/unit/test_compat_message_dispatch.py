"""Message-dispatch fast-path contracts for the Paho compatibility façade."""

from __future__ import annotations

import mqttium.compat.paho as paho_module
from mqttium.compat.paho import CallbackAPIVersion, Client
from mqttium.types import Message


def _has_message_dispatch(client: Client) -> bool:
    callback = client._async.on_message
    return (
        callback is not None
        and getattr(callback, "__self__", None) is client
        and getattr(callback, "__func__", None) is Client._dispatch_message
    )


def test_idle_compat_client_does_not_install_message_dispatch() -> None:
    client = Client(CallbackAPIVersion.VERSION2)

    assert client._topic_callbacks is None
    assert client._async.on_message is None
    assert client._async._message_callback is None
    assert client._async._delivery.mode == "callback"

    result = client._async._accept_message(
        Message(topic="ignored", payload=b"x"),
        client._async._message_callback,
    )
    assert result is None
    assert client._async._messages.empty()
    assert client._async._callback_queue.empty()
    assert client._async._callback_worker_task is None


def test_default_message_callback_installs_and_removes_dispatch() -> None:
    client = Client(CallbackAPIVersion.VERSION2)
    callback = lambda *_args: None

    client.on_message = callback
    assert client.on_message is callback
    assert _has_message_dispatch(client)

    client.on_message = None
    assert client.on_message is None
    assert client._async.on_message is None
    assert client._async._message_callback is None


def test_topic_callbacks_install_dispatch_only_while_needed() -> None:
    client = Client(CallbackAPIVersion.VERSION2)
    topic_callback = lambda *_args: None
    fallback = lambda *_args: None

    client.message_callback_add("sensors/#", topic_callback)
    assert client._topic_callbacks is not None
    assert _has_message_dispatch(client)

    client.on_message = fallback
    client.message_callback_remove("sensors/#")
    assert client._topic_callbacks is None
    assert _has_message_dispatch(client)

    client.on_message = None
    assert client._async.on_message is None
    assert client._async._message_callback is None


def test_topic_callback_remove_preserves_dispatch_until_last_filter() -> None:
    client = Client(CallbackAPIVersion.VERSION2)

    client.message_callback_remove("missing/#")
    assert client._topic_callbacks is None
    assert client._async.on_message is None

    client.message_callback_add("sensors/#", lambda *_args: None)
    client.message_callback_add("other/#", lambda *_args: None)
    client.message_callback_remove("other/#")

    assert client._topic_callbacks is not None
    assert _has_message_dispatch(client)
    client.message_callback_remove("sensors/#")
    assert client._topic_callbacks is None
    assert client._async.on_message is None


def test_filtered_dispatch_wraps_once_and_skips_unmatched_allocation(monkeypatch) -> None:
    client = Client(CallbackAPIVersion.VERSION2)
    wrapped: list[object] = []
    seen: list[object] = []
    original = paho_module.MQTTMessage

    class TrackingMessage(original):
        def __init__(self, message: Message) -> None:
            super().__init__(message)
            wrapped.append(self)

    monkeypatch.setattr(paho_module, "MQTTMessage", TrackingMessage)
    client.message_callback_add("sensors/#", lambda _c, _u, message: seen.append(message))
    client.message_callback_add("sensors/+", lambda _c, _u, message: seen.append(message))

    client._dispatch_message(Message(topic="other", payload=b"x"))
    assert wrapped == []
    assert seen == []

    client._dispatch_message(Message(topic="sensors/1", payload=b"x"))
    assert len(wrapped) == 1
    assert seen == [wrapped[0], wrapped[0]]


def test_message_dispatch_mutation_is_safe_after_loop_start() -> None:
    client = Client(CallbackAPIVersion.VERSION2)
    callback = lambda *_args: None
    client.loop_start()
    try:
        client.on_message = callback
        assert client._run_loop_mutation(lambda: _has_message_dispatch(client)) is True

        client.on_message = None
        assert client._run_loop_mutation(lambda: client._async.on_message) is None
    finally:
        client.loop_stop()
