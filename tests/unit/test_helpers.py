"""Tests for helpers.publish / helpers.subscribe (fake broker)."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.helpers import publish as publish_helper
from mqttium.helpers import subscribe as subscribe_helper
from mqttium.packets import PublishPacket
from mqttium.types import Message
from tests.support import ScriptedBrokerTransport


@pytest.mark.parametrize("msg_count", [0, -1])
async def test_subscribe_simple_rejects_non_positive_message_count(msg_count: int) -> None:
    with pytest.raises(ValueError, match="msg_count must be greater than 0"):
        await subscribe_helper.simple("topic", msg_count=msg_count)


async def test_publish_single_and_multiple(monkeypatch: pytest.MonkeyPatch) -> None:
    async def factory(host: str, port: int, *, ssl=None):
        return ScriptedBrokerTransport()

    from mqttium.api import async_client as ac

    monkeypatch.setattr(ac.TcpTransport, "connect", staticmethod(factory))

    await publish_helper.single("t/1", b"a", qos=1, hostname="fake", keepalive=5)
    await publish_helper.multiple(
        [
            {"topic": "t/2", "payload": b"b", "qos": 1},
            ("t/3", b"c", 0, False),
        ],
        hostname="fake",
        keepalive=5,
    )


async def test_subscribe_simple(monkeypatch: pytest.MonkeyPatch) -> None:
    inbound = PublishPacket(topic="news/1", payload=b"hi", qos=0, retain=False, dup=False).encode()

    async def factory(host: str, port: int, *, ssl=None):
        return ScriptedBrokerTransport(publish_after_subscribe=inbound)

    from mqttium.api import async_client as ac

    monkeypatch.setattr(ac.TcpTransport, "connect", staticmethod(factory))

    msg = await subscribe_helper.simple("news/#", hostname="fake", timeout=2.0, keepalive=5)
    assert not isinstance(msg, list)
    assert msg.topic == "news/1"
    assert msg.payload == b"hi"


async def test_subscribe_simple_collects_multiple_and_filters_retained(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retained = PublishPacket(
        topic="news/retained",
        payload=b"old",
        qos=0,
        retain=True,
        dup=False,
    ).encode()
    fresh = [
        PublishPacket(
            topic=f"news/{index}",
            payload=str(index).encode(),
            qos=0,
            retain=False,
            dup=False,
        ).encode()
        for index in range(2)
    ]

    async def factory(host: str, port: int, *, ssl=None):
        return ScriptedBrokerTransport(
            publish_after_subscribe=retained + b"".join(fresh),
        )

    from mqttium.api import async_client as ac

    monkeypatch.setattr(ac.TcpTransport, "connect", staticmethod(factory))

    messages = await subscribe_helper.simple(
        "news/#",
        msg_count=2,
        retained=False,
        hostname="fake",
        timeout=2.0,
    )
    assert isinstance(messages, list)
    assert [message.topic for message in messages] == ["news/0", "news/1"]


async def test_subscribe_callback_disconnects_when_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ControlledClient:
        def __init__(self) -> None:
            self.on_message = None
            self.subscribed = asyncio.Event()
            self.disconnected = asyncio.Event()

        async def connect(self, hostname: str, port: int, *, ssl=None) -> None:
            assert hostname == "fake"
            assert port == 1883

        async def subscribe(self, topic: str, *, qos: int) -> None:
            assert topic == "events/#"
            assert qos == 0
            assert self.on_message is not None
            self.on_message(Message(topic="events/1", payload=b"event"))
            self.subscribed.set()

        async def disconnect(self) -> None:
            self.disconnected.set()

    client = ControlledClient()
    monkeypatch.setattr(subscribe_helper, "create_client", lambda **_kwargs: client)
    seen: list[Message] = []
    task = asyncio.create_task(subscribe_helper.callback(seen.append, "events/#", hostname="fake"))
    await asyncio.wait_for(client.subscribed.wait(), timeout=1.0)

    assert [message.topic for message in seen] == ["events/1"]
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1.0)

    assert client.disconnected.is_set()
