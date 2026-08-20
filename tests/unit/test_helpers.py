"""Tests for helpers.publish / helpers.subscribe (fake broker)."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.helpers import publish as publish_helper
from mqttium.helpers import subscribe as subscribe_helper
from mqttium.packets import PublishPacket
from tests.support import ScriptedBrokerTransport, wait_until


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
    transport = ScriptedBrokerTransport()

    async def factory(host: str, port: int, *, ssl=None):
        return transport

    from mqttium.api import async_client as ac

    monkeypatch.setattr(ac.TcpTransport, "connect", staticmethod(factory))
    seen = []
    task = asyncio.create_task(subscribe_helper.callback(seen.append, "events/#", hostname="fake"))
    await wait_until(lambda: any(frame[0] >> 4 == 8 for frame in transport.written))

    transport.push_rx(
        PublishPacket(topic="events/1", payload=b"event", qos=0, retain=False, dup=False).encode()
    )
    await wait_until(lambda: len(seen) == 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert seen[0].topic == "events/1"
    assert transport.is_closing()
