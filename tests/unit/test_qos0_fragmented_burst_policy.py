"""Direct QoS0 fragments must not regain opportunistic reader ownership.

The network benchmark showed that a burst may reach the decoder as several
single-PUBLISH reads.  Treating each captured singleton as an isolated-message
fast path recovered singleton throughput but measurably worsened burst loop lag.
Keep the retained policy explicit: direct-decoded QoS0 notifications are
worker-owned even when transport chunking exposes them one at a time.
"""

from __future__ import annotations

import asyncio

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket
from mqttium.types import Message


def _publish(topic: str) -> bytes:
    return PublishPacket(
        topic=topic,
        payload=b"x",
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
    ).encode(MQTTProtocolVersion.MQTTv311)


async def test_successive_singleton_qos0_captures_remain_worker_owned() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=8)
    client._engine.state = ConnectionState.CONNECTED
    caller = asyncio.current_task()
    seen: list[str] = []
    owners: list[asyncio.Task[object] | None] = []

    def callback(message: Message) -> None:
        seen.append(message.topic)
        owners.append(asyncio.current_task())

    client.on_message = callback
    try:
        # Model a network burst fragmented across three reads. Each decode pass
        # sees a singleton, but none is allowed to become a reader-owned
        # callback merely because the transport split the burst this way.
        for topic in ("one", "two", "three"):
            client._decoder.feed(_publish(topic))
            handled, _, handoff, captured, sizes = client._process_direct_qos0_batch()
            assert handled == 1
            assert handoff is False
            assert [message.topic for message in captured] == [topic]
            assert client._delivery.deliver_callback_messages_inline(captured, callback, sizes)

        assert seen == []
        assert client._callback_queue.qsize() == 3
        await asyncio.wait_for(client._callback_queue.join(), 1)
        assert seen == ["one", "two", "three"]
        worker = client._callback_worker_task
        assert worker is not None
        assert all(owner is worker and owner is not caller for owner in owners)
    finally:
        await asyncio.wait_for(client._shutdown_callback_worker(drain=False), 1)
        await asyncio.wait_for(client._callback_queue.join(), 1)
