"""One-shot async subscribe helpers (Paho subscribe.simple / callback analogue)."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from typing import Any, Never

from mqttium.helpers._common import create_client, normalize_topics
from mqttium.enums import MQTTProtocolVersion
from mqttium.types import Message


async def simple(
    topics: str | Iterable[str],
    *,
    qos: int = 0,
    msg_count: int = 1,
    retained: bool = True,
    hostname: str = "localhost",
    port: int = 1883,
    client_id: str = "",
    keepalive: int = 60,
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    username: str | None = None,
    password: bytes | str | None = None,
    ssl: Any = None,
    timeout: float | None = 30.0,
) -> Message | list[Message]:
    """Subscribe, collect ``msg_count`` messages, disconnect.

    Returns a single ``Message`` when ``msg_count == 1``, else a list.
    """
    topic_list = normalize_topics(topics)
    client = create_client(
        client_id=client_id,
        protocol=protocol,
        keepalive=keepalive,
        username=username,
        password=password,
        message_delivery="callback",
    )
    collected: list[Message] = []
    done = asyncio.Event()

    def on_message(msg: Message) -> None:
        if msg.retain and not retained:
            return
        collected.append(msg)
        if len(collected) >= msg_count:
            done.set()

    client.on_message = on_message
    await client.connect(hostname, port, ssl=ssl)
    try:
        for topic in topic_list:
            await client.subscribe(topic, qos=qos)
        await asyncio.wait_for(done.wait(), timeout=timeout)
    finally:
        await client.disconnect()
    if msg_count == 1:
        return collected[0]
    return collected


async def callback(
    on_message: Callable[[Message], Any],
    topics: str | Iterable[str],
    *,
    qos: int = 0,
    hostname: str = "localhost",
    port: int = 1883,
    client_id: str = "",
    keepalive: int = 60,
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    username: str | None = None,
    password: bytes | str | None = None,
    ssl: Any = None,
) -> Never:
    """Subscribe and invoke ``on_message`` for every message (runs until cancelled)."""
    topic_list = normalize_topics(topics)
    client = create_client(
        client_id=client_id,
        protocol=protocol,
        keepalive=keepalive,
        username=username,
        password=password,
        message_delivery="callback",
    )
    client.on_message = on_message
    await client.connect(hostname, port, ssl=ssl)
    try:
        for topic in topic_list:
            await client.subscribe(topic, qos=qos)
        await asyncio.Event().wait()
    finally:
        await client.disconnect()
    raise AssertionError("unreachable")
