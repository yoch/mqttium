"""Targeted publish-admission wakeups: one ACK wakes one waiter."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import PublishMessage
from mqttium.api.async_client import AsyncClient
from mqttium.enums import PacketType
from mqttium.errors import MQTTError
from mqttium.packets import encode_frame
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.protocol.reconnect import ReconnectPolicy


async def _complete(client: AsyncClient, mid: int) -> None:
    stored = client._engine.store.get_out(mid)
    assert stored is not None
    client._engine.outbound.complete_record(mid, stored)
    client._engine.packet_ids.release(mid)
    await client._apply_effect(
        EngineEffect(kind=EffectKind.PUBLISH_COMPLETE, data=mid),
        nowait=False,
    )


async def _wait_until_parked(client: AsyncClient, n: int) -> None:
    for _ in range(50):
        if client._publish_waiters >= n:
            return
        await asyncio.sleep(0)
    raise AssertionError(f"expected {n} parked waiters, got {client._publish_waiters}")


def _parked_client(*, max_pending_outbound_messages: int = 1) -> AsyncClient:
    return AsyncClient(
        max_pending_outbound_messages=max_pending_outbound_messages,
        max_pending_outbound_bytes=None,
    )


class _ClosingTransport:
    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._closing = False

    async def write(self, data: bytes) -> None:
        if data and data[0] == PacketType.CONNECT:
            self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        self.drop()

    def is_closing(self) -> bool:
        return self._closing

    def drop(self) -> None:
        self._closing = True
        self._rx.put_nowait(b"")


async def _connect(client: AsyncClient) -> _ClosingTransport:
    transport = _ClosingTransport()

    async def factory(host: str, port: int, *, ssl: object = None) -> _ClosingTransport:
        return transport

    client._transport_factory = factory  # type: ignore[assignment]
    await client.connect("fake", 1883)
    return transport


async def test_wait_mode_blocks_until_logical_capacity_is_released() -> None:
    client = _parked_client()
    first = await client.publish("wake/first", b"one", qos=1)
    assert first.mid is not None

    second_task = asyncio.create_task(client.publish("wake/second", b"two", qos=1))
    await _wait_until_parked(client, 1)
    assert not second_task.done()

    await _complete(client, first.mid)
    second = await asyncio.wait_for(second_task, timeout=1.0)

    assert second.mid is not None
    assert client._publish_waiters == 0
    assert client._engine.pending_outbound_messages == 1


async def test_one_completion_releases_exactly_one_of_two_waiters() -> None:
    client = _parked_client()
    first = await client.publish("wake/held", b"one", qos=1)
    assert first.mid is not None

    left = asyncio.create_task(client.publish("wake/left", b"two", qos=1))
    right = asyncio.create_task(client.publish("wake/right", b"three", qos=1))
    await _wait_until_parked(client, 2)
    assert client._publish_wakeups == 0

    await _complete(client, first.mid)
    finished, remaining = await asyncio.wait(
        {left, right},
        timeout=1.0,
        return_when=asyncio.FIRST_COMPLETED,
    )
    assert len(finished) == 1
    assert len(remaining) == 1
    await asyncio.sleep(0)
    pending = next(iter(remaining))
    assert not pending.done()
    assert client._publish_waiters == 1
    assert client._engine.pending_outbound_messages == 1
    assert client._publish_wakeups == 1

    released = next(iter(finished)).result()
    assert released.mid is not None
    await _complete(client, released.mid)
    last = await asyncio.wait_for(pending, timeout=1.0)
    assert last.mid is not None
    assert client._publish_waiters == 0
    assert client._publish_wakeups == 2


async def test_two_completions_can_release_two_waiters() -> None:
    client = _parked_client(max_pending_outbound_messages=2)
    held = [
        await client.publish("wake/held-a", b"a", qos=1),
        await client.publish("wake/held-b", b"b", qos=1),
    ]
    assert held[0].mid is not None and held[1].mid is not None

    waiters = [
        asyncio.create_task(client.publish("wake/c", b"c", qos=1)),
        asyncio.create_task(client.publish("wake/d", b"d", qos=1)),
    ]
    await _wait_until_parked(client, 2)

    await _complete(client, held[0].mid)
    await _complete(client, held[1].mid)
    results = await asyncio.wait_for(asyncio.gather(*waiters), timeout=1.0)
    assert all(receipt.mid is not None for receipt in results)
    assert client._publish_waiters == 0
    assert client._publish_wakeups == 2
    assert client._engine.pending_outbound_messages == 2


async def test_cancelling_one_waiter_does_not_steal_the_wakeup() -> None:
    client = _parked_client()
    first = await client.publish("wake/held", b"one", qos=1)
    assert first.mid is not None

    victim = asyncio.create_task(client.publish("wake/victim", b"two", qos=1))
    other = asyncio.create_task(client.publish("wake/other", b"three", qos=1))
    await _wait_until_parked(client, 2)

    victim.cancel()
    with pytest.raises(asyncio.CancelledError):
        await victim
    assert client._publish_waiters == 1
    assert not other.done()

    await _complete(client, first.mid)
    released = await asyncio.wait_for(other, timeout=1.0)
    assert released.mid is not None
    assert client._publish_waiters == 0
    assert client._engine.pending_outbound_messages == 1
    assert client._publish_wakeups == 1


async def test_terminal_wakeup_fails_every_parked_publisher() -> None:
    client = AsyncClient(
        client_id="c",
        clean_start=False,
        max_pending_outbound_messages=1,
        max_pending_outbound_bytes=None,
        reconnect=ReconnectPolicy(enabled=False),
    )
    transport = await _connect(client)
    await client.publish("wake/first", b"one", qos=1)

    parked = [
        asyncio.create_task(client.publish("wake/a", b"a", qos=1)),
        asyncio.create_task(client.publish("wake/b", b"b", qos=1)),
    ]
    await _wait_until_parked(client, 2)
    transport.drop()

    for task in parked:
        with pytest.raises(MQTTError):
            await asyncio.wait_for(task, timeout=1.0)
    assert client._publish_waiters == 0


async def test_publish_many_waits_for_one_slot_then_admits_the_chunk() -> None:
    client = _parked_client()
    first = await client.publish("wake/held", b"one", qos=1)
    assert first.mid is not None

    batch = asyncio.create_task(client.publish_many([PublishMessage("wake/batch", b"two", qos=1)]))
    await _wait_until_parked(client, 1)
    assert not batch.done()

    await _complete(client, first.mid)
    receipt = await asyncio.wait_for(batch, timeout=1.0)
    assert receipt.submitted == 1
    assert client._publish_waiters == 0
    assert client._engine.pending_outbound_messages == 1
    assert client._publish_wakeups == 1


async def test_publish_many_is_woken_once_per_ack_until_the_chunk_fits() -> None:
    client = _parked_client(max_pending_outbound_messages=2)
    held = [
        await client.publish("wake/held-a", b"a", qos=1),
        await client.publish("wake/held-b", b"b", qos=1),
    ]
    assert held[0].mid is not None and held[1].mid is not None

    batch = asyncio.create_task(
        client.publish_many(
            [
                PublishMessage("wake/batch-a", b"c", qos=1),
                PublishMessage("wake/batch-b", b"d", qos=1),
            ]
        )
    )
    await _wait_until_parked(client, 1)

    await _complete(client, held[0].mid)
    for _ in range(50):
        if client._publish_wait_retries >= 1 and client._publish_waiter_futs:
            break
        await asyncio.sleep(0)
    else:
        raise AssertionError("batch did not re-park after the first slot opened")
    assert not batch.done(), "chunk of 2 still needs the second slot"
    assert client._publish_waiters == 1
    assert client._publish_wakeups == 1
    assert client._engine.pending_outbound_messages == 1

    await _complete(client, held[1].mid)
    receipt = await asyncio.wait_for(batch, timeout=1.0)
    assert receipt.submitted == 2
    assert client._publish_waiters == 0
    assert client._publish_wakeups == 2


async def test_fairness_no_starvation_while_completions_keep_arriving() -> None:
    producers = 4
    per_producer = 3
    client = _parked_client()
    completed = [0] * producers
    stop = asyncio.Event()

    async def producer(index: int) -> None:
        for _ in range(per_producer):
            receipt = await client.publish(f"wake/p/{index}", b"x", qos=1)
            assert receipt.mid is not None
            completed[index] += 1

    async def completer() -> None:
        while not stop.is_set() or client._receipts:
            mids = list(client._receipts)
            if not mids:
                await asyncio.sleep(0)
                continue
            await _complete(client, mids[0])
            await asyncio.sleep(0)

    tasks = [asyncio.create_task(producer(index)) for index in range(producers)]
    drain = asyncio.create_task(completer())
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=2.0)
    stop.set()
    await asyncio.wait_for(drain, timeout=1.0)

    assert completed == [per_producer] * producers
    assert client._publish_waiters == 0
    assert client._publish_wakeups <= producers * per_producer
    assert min(completed) == per_producer


async def test_wakeups_do_not_exceed_completions_plus_teardown() -> None:
    client = _parked_client()
    first = await client.publish("wake/held", b"one", qos=1)
    assert first.mid is not None
    parked = [
        asyncio.create_task(client.publish("wake/a", b"a", qos=1)),
        asyncio.create_task(client.publish("wake/b", b"b", qos=1)),
        asyncio.create_task(client.publish("wake/c", b"c", qos=1)),
    ]
    await _wait_until_parked(client, 3)
    await _complete(client, first.mid)
    finished, remaining = await asyncio.wait(
        set(parked),
        timeout=1.0,
        return_when=asyncio.FIRST_COMPLETED,
    )
    assert len(finished) == 1
    assert client._publish_wakeups == 1
    await asyncio.sleep(0)
    assert sum(task.done() for task in parked) == 1
    for task in remaining:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    await next(iter(finished))
    assert client._publish_waiters == 0
