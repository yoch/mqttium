"""AsyncClient integration tests over an in-memory transport.

Covers the planning-audit findings: receipt registration must precede wire
emission (no PUBACK race) and the single-writer pipeline must complete a full
connect/publish/subscribe/disconnect cycle.
"""

from __future__ import annotations

import asyncio

from mqttium.api.async_client import AsyncClient
from mqttium.enums import QoS
from mqttium.packets import PublishPacket
from tests.support import ScriptedBrokerTransport, transport_factory


def _client_with_fake() -> tuple[AsyncClient, ScriptedBrokerTransport]:
    client = AsyncClient(client_id="test")
    fake = ScriptedBrokerTransport(suback_reason=1)
    client._transport_factory = transport_factory(fake)
    return client, fake


async def test_connect_publish_qos1_wait_disconnect() -> None:
    client, fake = _client_with_fake()
    connack = await client.connect("fake", 1883, timeout=2.0)
    assert connack.reason_code == 0
    assert client.is_connected

    receipt = await client.publish("t/1", b"hello", qos=1)
    # The fake broker acks synchronously on write: this only completes if the
    # receipt was registered before the packet hit the wire (audit finding B1).
    await asyncio.wait_for(receipt.wait(), timeout=2.0)
    assert receipt.is_done()

    result = await client.subscribe("t/#", qos=1)
    assert result.mid > 0
    assert result.reason_codes == (1,)
    await client.disconnect()
    assert not client.is_connected


async def test_many_concurrent_publishes_complete() -> None:
    client, fake = _client_with_fake()
    await client.connect("fake", 1883, timeout=2.0)

    receipts = await asyncio.gather(*(client.publish(f"t/{i}", str(i), qos=1) for i in range(50)))
    await asyncio.wait_for(asyncio.gather(*(r.wait() for r in receipts)), timeout=5.0)
    assert all(r.is_done() for r in receipts)
    await client.disconnect()


async def test_qos0_receipt_immediate() -> None:
    client, fake = _client_with_fake()
    await client.connect("fake", 1883, timeout=2.0)
    receipt = await client.publish("t/0", b"fire-and-forget", qos=0)
    assert receipt.is_done()
    await asyncio.wait_for(receipt.wait(), timeout=1.0)
    await client.disconnect()


async def test_full_message_queue_close_loses_nothing() -> None:
    """P1.7: the end-of-stream sentinel must never evict a queued message."""
    client = AsyncClient(client_id="test", max_pending_messages=4, message_delivery="iterator")
    fake = ScriptedBrokerTransport()
    client._transport_factory = transport_factory(fake)
    await client.connect("fake", 1883, timeout=2.0)

    for i in range(4):
        fake._rx.put_nowait(
            PublishPacket(
                topic=f"t/{i}",
                payload=b"x",
                qos=QoS.AT_MOST_ONCE,
                retain=False,
                dup=False,
                mid=None,
            ).encode()
        )
    for _ in range(100):
        if client._messages.qsize() == 4:
            break
        await asyncio.sleep(0.01)
    assert client._messages.qsize() == 4

    await fake.close()
    # Wait for the reader's cleanup so the sentinel is inserted while the
    # queue is still full (otherwise draining first would mask the eviction).
    reader = client._reader_task
    assert reader is not None
    await asyncio.wait_for(reader, timeout=2.0)
    received = []
    async for msg in client.messages():
        received.append(msg.topic)
    assert received == ["t/0", "t/1", "t/2", "t/3"]


async def test_sequential_qos1_receipts_survive_immediate_mid_reuse() -> None:
    """A completed MID can be reused before its completion effect is applied."""
    client, fake = _client_with_fake()
    await client.connect("fake", 1883, timeout=2.0)

    receipts = []
    for index in range(250):
        receipts.append(await client.publish("reuse/qos1", str(index), qos=1))
        if len(receipts) >= 100:
            await asyncio.wait_for(receipts[-100].wait(), timeout=2.0)

    await asyncio.wait_for(asyncio.gather(*(receipt.wait() for receipt in receipts)), timeout=5.0)
    assert all(receipt.is_done() for receipt in receipts)
    assert not client._receipts
    await client.disconnect()
