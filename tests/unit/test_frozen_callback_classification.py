"""Message routes validate sync-only invocation before dispatch begins."""

from __future__ import annotations

import asyncio
from functools import partial, wraps

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import PacketType, QoS
from mqttium.packets import PubCompPacket, PublishPacket, PubRecPacket
from mqttium.types import Message
from tests.support import ScriptedBrokerTransport, deliver_message, transport_factory


@pytest.mark.parametrize(
    "kind",
    ["sync", "partial", "bound", "object"],
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
                await deliver_message(client, Message(topic=topic, payload=topic.encode()))
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
        await deliver_message(client, Message(topic="t/a", payload=b"x"))
        assert seen == ["later route"]
        assert len(errors) == 1
        assert errors[0]["callback"] is bad
        assert not future.done()
        if returned:
            assert returned[0].cr_frame is None
    finally:
        await client.disconnect()
        loop.set_exception_handler(previous)


async def test_receipts_complete_without_invoking_message_callbacks():
    class CompletionBroker(ScriptedBrokerTransport):
        def handle_packet(self, raw):
            super().handle_packet(raw)
            if raw.packet_type is PacketType.PUBLISH:
                packet = PublishPacket.decode(raw.flags, raw.remaining, self.protocol)
                if packet.qos is QoS.EXACTLY_ONCE:
                    self.push_rx(PubRecPacket(packet.mid).encode(self.protocol))
            elif raw.packet_type is PacketType.PUBREL:
                mid = int.from_bytes(raw.remaining[:2], "big")
                self.push_rx(PubCompPacket(mid).encode(self.protocol))

    client = AsyncClient("mutable-notifications", message_delivery="callback")
    client.on_message = lambda _: None
    client._transport_factory = transport_factory(CompletionBroker())
    try:
        await client.connect("fake")
        assert not hasattr(client, "on_publish")
        for qos in (0, 1, 2):
            receipt = await client.publish("t", b"x", qos=qos)
            await asyncio.wait_for(receipt.wait(), 1)
        assert client._delivery.callback_invocations == 0
        assert client.stats().delivery.callback_invocations == 0
    finally:
        await client.disconnect()


@pytest.mark.parametrize(
    "kind", ["function", "partial", "bound", "object", "object-partial", "async-generator"]
)
@pytest.mark.parametrize("route", [False, True])
def test_async_callback_rejected_without_mutating_registration(kind, route):
    client = AsyncClient(message_delivery="callback")

    async def function(_message):
        pass

    async def generator(message):
        yield message

    class AsyncCallable:
        async def __call__(self, _message):
            pass

        async def method(self, _message):
            pass

    obj = AsyncCallable()
    callback = {
        "function": function,
        "partial": partial(function),
        "bound": obj.method,
        "object": obj,
        "object-partial": partial(obj),
        "async-generator": generator,
    }[kind]
    previous = lambda _message: None
    client.on_message = previous
    client.message_callback_add("t/#", previous)
    with pytest.raises(TypeError, match=r"synchronous; use messages\(\)"):
        if route:
            client.message_callback_add("t/#", callback)
        else:
            client.on_message = callback
    assert client.on_message is previous
    assert client._topic_callbacks["t/#"] is previous


def test_on_message_non_callable_rejected_without_mutation():
    client = AsyncClient(message_delivery="callback")
    with pytest.raises(TypeError, match="callable"):
        client.on_message = 1
    assert client.on_message is None
