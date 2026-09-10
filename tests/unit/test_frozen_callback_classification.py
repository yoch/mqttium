"""Frozen routes classify invocation once while preserving callback contracts."""

from __future__ import annotations

import asyncio
from functools import partial, wraps

import pytest

from mqttium.api import AsyncClient
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message
from tests.support import ScriptedBrokerTransport, transport_factory


@pytest.mark.parametrize(
    "kind",
    ["sync", "async", "partial", "async-partial", "bound", "async-bound", "object", "async-object"],
)
@pytest.mark.parametrize("routes", [False, True])
async def test_frozen_callbacks_are_not_reclassified_after_connect(monkeypatch, kind, routes):
    client = AsyncClient("classified", message_delivery="callback")
    seen = []

    def sync(message):
        seen.append(message.payload)

    async def asynchronous(message):
        await asyncio.sleep(0)
        seen.append(message.payload)

    class SyncCallable:
        __hash__ = None

        def __call__(self, message):
            sync(message)

        def method(self, message):
            sync(message)

    class AsyncCallable:
        __hash__ = None

        async def __call__(self, message):
            await asynchronous(message)

        async def method(self, message):
            await asynchronous(message)

    callbacks = {
        "sync": sync,
        "async": asynchronous,
        "partial": partial(sync),
        "async-partial": partial(asynchronous),
        "bound": SyncCallable().method,
        "async-bound": AsyncCallable().method,
        "object": SyncCallable(),
        "async-object": AsyncCallable(),
    }
    callback = callbacks[kind]
    client.on_message = callback
    if routes:
        client.message_callback_add("t/#", callback)
        client.message_callback_add("t/+", callback)
        client.message_callback_add("t/exact", callback)
    client._transport_factory = transport_factory(ScriptedBrokerTransport())
    await client.connect("fake")

    def unexpected_classification(_callback):
        raise AssertionError("frozen message routes must not be reclassified")

    monkeypatch.setattr(
        type(client._delivery), "_is_async_callback", staticmethod(unexpected_classification)
    )
    try:
        for _ in range(2):
            assert client.on_message is callback
            for topic in ("t/exact", "fallback"):
                await client._apply_effect(
                    EngineEffect(EffectKind.MESSAGE, Message(topic=topic, payload=topic.encode())),
                    nowait=False,
                )
            await client._delivery.callback_queue.join()
            await client.disconnect()
            client._transport_factory = transport_factory(ScriptedBrokerTransport())
            await client.connect("fake")
        expected = [b"t/exact"] * (3 if routes else 1) + [b"fallback"]
        assert seen == expected * 2
    finally:
        await client.disconnect()


@pytest.mark.parametrize("kind", ["coroutine", "future", "cancelled", "runtime", "wrapped"])
async def test_frozen_bad_callback_is_isolated_with_original_identity(kind):
    client = AsyncClient("classified-errors", message_delivery="callback")
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    errors, seen, returned = [], [], []
    future = loop.create_future()
    loop.set_exception_handler(lambda _, context: errors.append(context))

    async def body(_message):
        seen.append("invalid coroutine ran")

    def bad(message):
        if kind == "future":
            return future
        if kind == "cancelled":
            raise asyncio.CancelledError("callback cancelled")
        if kind == "runtime":
            raise RuntimeError("callback failed")
        coroutine = body(message)
        returned.append(coroutine)
        return coroutine

    if kind == "wrapped":
        bad = wraps(body)(bad)
    client.message_callback_add("t/#", bad)
    client.message_callback_add("t/+", lambda _: seen.append("later route"))
    client._transport_factory = transport_factory(ScriptedBrokerTransport())
    try:
        await client.connect("fake")
        await client._apply_effect(
            EngineEffect(EffectKind.MESSAGE, Message(topic="t/a", payload=b"x")), nowait=False
        )
        await client._delivery.callback_queue.join()
        assert seen == ["later route"]
        assert len(errors) == 1
        assert errors[0]["callback"] is bad
        assert not future.done()
        if returned:
            assert returned[0].cr_frame is None
    finally:
        await client.disconnect()
        loop.set_exception_handler(previous)


async def test_publish_notifications_remain_mutable_after_route_freeze():
    client = AsyncClient("mutable-notifications", message_delivery="callback")
    client.on_message = lambda _: None
    client._transport_factory = transport_factory(ScriptedBrokerTransport())
    seen = []

    async def asynchronous(mid, error):
        await asyncio.sleep(0)
        seen.append(("async", mid, error))

    try:
        await client.connect("fake")
        client.on_publish = lambda mid, error: seen.append(("sync", mid, error))
        await client.publish("t", b"x")
        await client._delivery.callback_queue.join()
        client.on_publish = asynchronous
        await client.publish("t", b"y")
        await client._delivery.callback_queue.join()
        assert seen == [("sync", None, None), ("async", None, None)]
    finally:
        await client.disconnect()
