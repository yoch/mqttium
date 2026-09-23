"""Deferred request results must settle before their identifiers are reused."""

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import NotConnectedError
from mqttium.packets import PublishPacket, encode_frame
from tests.support import QueueTransport, wait_until


class _PausedRequestTransport(QueueTransport):
    def __init__(self, protocol: MQTTProtocolVersion) -> None:
        super().__init__()
        self.protocol = protocol
        self.decoder = IncrementalDecoder()
        self.requests: list[int] = []
        self.gate = asyncio.Event()
        self.gate.set()
        self.write_held = asyncio.Event()
        self.close_entered = asyncio.Event()
        self.allow_close: asyncio.Event | None = None

    async def write(self, data: bytes) -> None:
        if not self.gate.is_set():
            self.write_held.set()
        await self.gate.wait()
        self.decoder.feed(data)
        for raw in self.decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                self.push_rx(encode_frame(PacketType.CONNACK, 0, b"\x00\x00" + self.properties))
            elif raw.packet_type in (PacketType.SUBSCRIBE, PacketType.UNSUBSCRIBE):
                self.requests.append(int.from_bytes(raw.remaining[:2], "big"))

    @property
    def properties(self) -> bytes:
        return b"\x00" if self.protocol is MQTTProtocolVersion.MQTTv5 else b""

    def acknowledgement(self, mid: int, unsubscribe: bool) -> bytes:
        reasons = b"\x00" if not unsubscribe or self.properties else b""
        kind = PacketType.UNSUBACK if unsubscribe else PacketType.SUBACK
        return encode_frame(kind, 0, mid.to_bytes(2, "big") + self.properties + reasons)

    async def close(self) -> None:
        self.gate.set()
        if self.allow_close is not None:
            self.close_entered.set()
            await self.allow_close.wait()
        await super().close()


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
@pytest.mark.parametrize("unsubscribe", [False, True])
async def test_deferred_ack_cannot_complete_reused_identifier(protocol, unsubscribe) -> None:
    transport = _PausedRequestTransport(protocol)
    client = AsyncClient(
        "request-reuse", protocol=protocol, keepalive=0, max_write_queue_messages=1
    )

    async def factory(*args, **kwargs):
        return transport

    client._transport_factory = factory
    await client.connect("unused")
    request = client.unsubscribe if unsubscribe else client.subscribe
    tasks = []
    try:
        first = asyncio.create_task(request("first"))
        tasks.append(first)
        await wait_until(lambda: len(transport.requests) == 1)
        mid = transport.requests[0]
        transport.gate.clear()
        await client.publish("block", b"x")
        await transport.write_held.wait()
        incoming = PublishPacket(
            topic="incoming", payload=b"x", qos=QoS.AT_LEAST_ONCE, mid=7, retain=False, dup=False
        ).encode(protocol)
        transport.push_rx(transport.acknowledgement(mid, unsubscribe) + incoming)
        await wait_until(lambda: client._write_pump.waiters == 1)
        second = asyncio.create_task(request("second"))
        tasks.append(second)
        await wait_until(lambda: client._effect_pump.waiters >= 2)
        transport.gate.set()
        await wait_until(lambda: len(transport.requests) == 2)
        assert transport.requests == [mid, mid]
        assert not second.done(), "The second exchange has not received an acknowledgement"
        assert (await asyncio.wait_for(first, 1)).mid == mid
        transport.push_rx(transport.acknowledgement(mid, unsubscribe))
        assert (await asyncio.wait_for(second, 1)).mid == mid
    finally:
        transport.gate.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await client.disconnect()


_MATRIX = [
    pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5]),
    pytest.mark.parametrize("unsubscribe", [False, True]),
]


def _matrix(test):
    for mark in _MATRIX:
        test = mark(test)
    return test


async def _connected(protocol):
    transport = _PausedRequestTransport(protocol)
    client = AsyncClient(
        "request-window", protocol=protocol, keepalive=0, max_write_queue_messages=1
    )

    async def factory(*args, **kwargs):
        return transport

    client._transport_factory = factory
    await client.connect("unused")
    return transport, client


async def _saturate_writer(transport, client):
    transport.write_held.clear()
    transport.gate.clear()
    await client.publish("block", b"x")
    await transport.write_held.wait()


def _incoming(protocol, mid):
    return PublishPacket(
        topic="incoming", payload=b"x", qos=QoS.AT_LEAST_ONCE, mid=mid, retain=False, dup=False
    ).encode(protocol)


async def _second_request_in_drain(transport, client, request):
    """Start a second request that must wait on an unrelated deferred PUBACK."""
    await _saturate_writer(transport, client)
    transport.push_rx(_incoming(transport.protocol, 7))
    await wait_until(lambda: client._write_pump.waiters == 1)
    second = asyncio.create_task(request("second"))
    await wait_until(lambda: client._effect_pump.waiters >= 1)
    return second


def _pause_after_drain(client, owner):
    """Hold ``owner`` between a completed drain and its next engine-lock check."""
    drain = client._effect_pump.drain
    reached = asyncio.Event()
    resume = asyncio.Event()
    calls = [0]

    async def paused_drain(*args, **kwargs):
        if asyncio.current_task() is owner():
            calls[0] += 1
        await drain(*args, **kwargs)
        if asyncio.current_task() is owner() and not reached.is_set():
            reached.set()
            await resume.wait()

    client._effect_pump.drain = paused_drain
    return reached, resume, calls


@_matrix
async def test_ack_released_between_drain_and_lock_is_settled_first(protocol, unsubscribe):
    transport, client = await _connected(protocol)
    request = client.unsubscribe if unsubscribe else client.subscribe
    futures = client._unsub_futs if unsubscribe else client._sub_futs
    tasks = []
    try:
        first = asyncio.create_task(request("first"))
        tasks.append(first)
        await wait_until(lambda: len(transport.requests) == 1)
        mid = transport.requests[0]
        reached, resume, drains = _pause_after_drain(
            client, lambda: tasks[1] if len(tasks) > 1 else None
        )
        tasks.append(await _second_request_in_drain(transport, client, request))
        second = tasks[1]
        transport.gate.set()
        await reached.wait()

        # The old ACK frees its identifier after the drain and before the lock.
        await _saturate_writer(transport, client)
        transport.push_rx(transport.acknowledgement(mid, unsubscribe) + _incoming(protocol, 8))
        await wait_until(lambda: client._write_pump.waiters == 1)
        assert not client._engine.outbound.packet_ids.in_use(mid)
        assert not first.done()
        resume.set()
        # The lock recheck must send the request back to drain, not allocate.
        await wait_until(lambda: drains[0] >= 2 or client._engine.outbound.packet_ids.in_use(mid))
        assert not client._engine.outbound.packet_ids.in_use(mid)
        assert transport.requests == [mid]
        assert list(futures) == [mid]

        transport.gate.set()
        await wait_until(lambda: len(transport.requests) == 2)
        assert transport.requests == [mid, mid]
        assert (await asyncio.wait_for(first, 1)).mid == mid
        assert not second.done()
        transport.push_rx(transport.acknowledgement(mid, unsubscribe))
        assert (await asyncio.wait_for(second, 1)).mid == mid
    finally:
        transport.gate.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await client.disconnect()


@_matrix
async def test_fail_stop_between_drain_and_lock_admits_nothing(protocol, unsubscribe):
    transport, client = await _connected(protocol)
    request = client.unsubscribe if unsubscribe else client.subscribe
    futures = client._unsub_futs if unsubscribe else client._sub_futs
    tasks = []
    transport.allow_close = asyncio.Event()
    try:
        reached, resume, _ = _pause_after_drain(client, lambda: tasks[0] if tasks else None)
        tasks.append(await _second_request_in_drain(transport, client, request))
        pending = tasks[0]
        transport.gate.set()
        await reached.wait()

        # A second CONNACK is a protocol violation; teardown then blocks in close().
        transport.push_rx(encode_frame(PacketType.CONNACK, 0, b"\x00\x00" + transport.properties))
        await asyncio.wait_for(transport.close_entered.wait(), 1)
        identifiers = len(client._engine.outbound.packet_ids)
        resume.set()
        with pytest.raises(NotConnectedError):
            await asyncio.wait_for(pending, 1)
        assert not transport.allow_close.is_set()
        assert transport.requests == []
        assert not futures
        assert len(client._engine.outbound.packet_ids) == identifiers
    finally:
        transport.allow_close.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await client.disconnect()


@_matrix
async def test_cancellation_before_admission_admits_nothing(protocol, unsubscribe):
    transport, client = await _connected(protocol)
    request = client.unsubscribe if unsubscribe else client.subscribe
    futures = client._unsub_futs if unsubscribe else client._sub_futs
    tasks = []
    try:
        first = asyncio.create_task(request("first"))
        tasks.append(first)
        await wait_until(lambda: len(transport.requests) == 1)
        mid = transport.requests[0]
        second = await _second_request_in_drain(transport, client, request)
        tasks.append(second)
        identifiers = len(client._engine.outbound.packet_ids)
        second.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second
        assert list(futures) == [mid]
        assert len(client._engine.outbound.packet_ids) == identifiers

        transport.gate.set()
        await wait_until(lambda: client._write_pump.waiters == 0)
        transport.push_rx(transport.acknowledgement(mid, unsubscribe))
        assert (await asyncio.wait_for(first, 1)).mid == mid
        assert transport.requests == [mid]
        assert not futures
    finally:
        transport.gate.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await client.disconnect()


@_matrix
@pytest.mark.parametrize("pressure", [False, True])
async def test_one_shot_topic_iterable_is_consumed_once(protocol, unsubscribe, pressure):
    transport, client = await _connected(protocol)
    request = client.unsubscribe if unsubscribe else client.subscribe
    consumed = []

    def topics():
        # A generator is a valid Iterable request and can only be read once.
        for topic in ("one/a", "one/b"):
            consumed.append(topic)
            yield topic

    task = None
    try:
        if pressure:
            # Admission must wait on an unrelated deferred effect and retry.
            await _saturate_writer(transport, client)
            transport.push_rx(_incoming(protocol, 7))
            await wait_until(lambda: client._write_pump.waiters == 1)
        task = asyncio.create_task(request(topics()))
        if pressure:
            await wait_until(lambda: client._effect_pump.waiters >= 1)
            assert not transport.requests
            transport.gate.set()
        await wait_until(lambda: len(transport.requests) == 1)
        mid = transport.requests[0]
        codes = b"\x00\x00" if protocol is MQTTProtocolVersion.MQTTv5 or not unsubscribe else b""
        properties = transport.properties
        kind = PacketType.UNSUBACK if unsubscribe else PacketType.SUBACK
        transport.push_rx(encode_frame(kind, 0, mid.to_bytes(2, "big") + properties + codes))
        result = await asyncio.wait_for(task, 1)
        assert result.mid == mid
        assert consumed == ["one/a", "one/b"]
    finally:
        transport.gate.set()
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await client.disconnect()
