"""Cancelled callers do not retain futures or abandon protocol identifiers."""

import asyncio
import gc

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.errors import MQTTTimeoutError
from mqttium.packets import encode_frame
from tests.support import ScriptedBrokerTransport, transport_factory, wait_until

_PROTOCOLS = [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5]


@pytest.fixture(autouse=True)
async def _no_leaked_tasks_or_unretrieved_errors():
    loop = asyncio.get_running_loop()
    reported = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: reported.append(context))
    try:
        yield
        gc.collect()
        assert not reported
        assert asyncio.all_tasks() == {asyncio.current_task()}
    finally:
        loop.set_exception_handler(previous)


class _HeldAckTransport(ScriptedBrokerTransport):
    """Broker that records requests and acknowledges only on demand."""

    def __init__(self, protocol):
        super().__init__(protocol=protocol)
        self.gate = asyncio.Event()
        self.gate.set()
        self.held = asyncio.Event()
        self.requests: list[int] = []

    async def write(self, data):
        if not self.gate.is_set():
            self.held.set()
        await self.gate.wait()
        await super().write(data)

    def handle_packet(self, raw):
        if raw.packet_type in (PacketType.SUBSCRIBE, PacketType.UNSUBSCRIBE):
            self.requests.append(int.from_bytes(raw.remaining[:2], "big"))
        else:
            super().handle_packet(raw)

    def ack(self, mid, unsubscribe):
        v5 = self.protocol is MQTTProtocolVersion.MQTTv5
        properties = b"\x00" if v5 else b""
        reasons = b"\x00" if v5 or not unsubscribe else b""
        kind = PacketType.UNSUBACK if unsubscribe else PacketType.SUBACK
        return encode_frame(kind, 0, mid.to_bytes(2, "big") + properties + reasons)

    async def close(self):
        self.gate.set()
        await super().close()


class _Phases:
    """Observe the request task's post-admission transfer explicitly."""

    def __init__(self, client, futures):
        self.task = None
        self.transfer_entered = asyncio.Event()
        self.transfer_done = asyncio.Event()
        self.resume = asyncio.Event()
        self.resume.set()
        drain = client._effect_pump.drain

        async def observed_drain(*args, **kwargs):
            # The admission drain runs before registration; the transfer drain
            # runs once this caller owns a registered future.
            mine = asyncio.current_task() is self.task and bool(futures)
            if mine:
                self.transfer_entered.set()
            await drain(*args, **kwargs)
            if mine:
                self.transfer_done.set()
                await self.resume.wait()

        client._effect_pump.drain = observed_drain


async def _connected(protocol, unsubscribe):
    transport = _HeldAckTransport(protocol)
    client = AsyncClient(
        "request-cancel", protocol=protocol, keepalive=0, max_write_queue_messages=1
    )
    client._transport_factory = transport_factory(transport)
    await client.connect("unused")
    request = client.unsubscribe if unsubscribe else client.subscribe
    futures = client._unsub_futs if unsubscribe else client._sub_futs
    return transport, client, request, futures


async def _hold_writer(transport, client):
    transport.gate.clear()
    await client.publish("hold", b"x")
    await transport.held.wait()


async def _finish(transport, client, *tasks):
    transport.gate.set()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await client.disconnect()


@pytest.mark.parametrize("protocol", _PROTOCOLS)
@pytest.mark.parametrize("unsubscribe", [False, True])
@pytest.mark.parametrize("phase", ["transfer", "ack_wait"])
async def test_cancelled_request_keeps_protocol_owner_only(protocol, unsubscribe, phase):
    transport, client, request, futures = await _connected(protocol, unsubscribe)
    phases = _Phases(client, futures)
    tasks = []
    try:
        if phase == "transfer":
            await _hold_writer(transport, client)
        phases.task = asyncio.create_task(request("old"))
        tasks.append(phases.task)
        if phase == "transfer":
            await phases.transfer_entered.wait()
            assert not phases.transfer_done.is_set()
        else:
            await phases.transfer_done.wait()
            await wait_until(lambda: len(transport.requests) == 1)
        [(mid, future)] = futures.items()
        assert not future.done()

        phases.task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await phases.task
        assert future.cancelled()
        assert not futures
        assert client._engine.outbound.packet_ids.in_use(mid)

        transport.gate.set()
        await wait_until(lambda: transport.requests == [mid])
        # The engine still owns the old identifier, so a new request gets another.
        other = asyncio.create_task(request("new"))
        tasks.append(other)
        await wait_until(lambda: len(transport.requests) == 2)
        new_mid = transport.requests[1]
        assert new_mid != mid

        transport.push_rx(transport.ack(mid, unsubscribe))
        await wait_until(lambda: not client._engine.outbound.packet_ids.in_use(mid))
        assert list(futures) == [new_mid]
        assert not other.done()
        assert client.is_connected

        transport.push_rx(transport.ack(new_mid, unsubscribe))
        assert (await asyncio.wait_for(other, 1)).mid == new_mid
        assert not futures
    finally:
        await _finish(transport, client, *tasks)


@pytest.mark.parametrize("protocol", _PROTOCOLS)
@pytest.mark.parametrize("unsubscribe", [False, True])
@pytest.mark.parametrize("cancel", [False, True])
async def test_ack_available_before_wait(protocol, unsubscribe, cancel):
    transport, client, request, futures = await _connected(protocol, unsubscribe)
    phases = _Phases(client, futures)
    phases.resume.clear()
    tasks = []
    try:
        phases.task = asyncio.create_task(request("t"))
        tasks.append(phases.task)
        await phases.transfer_done.wait()
        await wait_until(lambda: len(transport.requests) == 1)
        [(mid, future)] = futures.items()
        transport.push_rx(transport.ack(mid, unsubscribe))
        await wait_until(future.done)
        assert not client._engine.outbound.packet_ids.in_use(mid)

        if cancel:
            phases.task.cancel()
        phases.resume.set()
        if cancel:
            with pytest.raises(asyncio.CancelledError):
                await phases.task
        else:
            assert (await phases.task).mid == mid
        assert not futures
    finally:
        await _finish(transport, client, *tasks)


@pytest.mark.parametrize("protocol", _PROTOCOLS)
@pytest.mark.parametrize("unsubscribe", [False, True])
async def test_ack_wait_timeout_keeps_identifier_until_late_ack(protocol, unsubscribe):
    transport, client, request, futures = await _connected(protocol, unsubscribe)
    phases = _Phases(client, futures)
    phases.resume.clear()
    tasks = []
    try:
        phases.task = asyncio.create_task(request("t", timeout=0.01))
        tasks.append(phases.task)
        await phases.transfer_done.wait()
        [(mid, future)] = futures.items()
        phases.resume.set()
        with pytest.raises(MQTTTimeoutError):
            await phases.task
        assert future.cancelled()
        assert not futures
        assert client._engine.outbound.packet_ids.in_use(mid)

        await wait_until(lambda: transport.requests == [mid])
        transport.push_rx(transport.ack(mid, unsubscribe))
        await wait_until(lambda: not client._engine.outbound.packet_ids.in_use(mid))
        assert not futures
        assert client.is_connected
    finally:
        await _finish(transport, client, *tasks)


@pytest.mark.parametrize("failure", [TimeoutError("transfer"), OSError("transfer")])
async def test_failed_transfer_preserves_cause_and_retrieves_future_failure(monkeypatch, failure):
    client = AsyncClient("transfer-failure")
    future = asyncio.get_running_loop().create_future()
    futures = {1: future}
    owned = [future]

    async def fail_drain():
        owned[0].set_exception(failure)
        raise failure

    monkeypatch.setattr(client._effect_pump, "drain", fail_drain)
    with pytest.raises(type(failure)) as caught:
        await client._await_request_ack(future, futures, 1, 1, "SUBACK")
    assert caught.value is failure
    assert not isinstance(caught.value, MQTTTimeoutError)
    assert not futures
    # The helper retrieves the discarded future's error as well as propagating
    # the drain error; destroying that future must not report it again.
    observed = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: observed.append(context))
    try:
        failure.__traceback__ = None
        owned.clear()
        del future
        gc.collect()
        assert not observed
    finally:
        loop.set_exception_handler(previous)


async def test_cleanup_never_removes_a_later_future_for_the_same_identifier(monkeypatch):
    client = AsyncClient("identity")
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    later = loop.create_future()
    futures = {1: future}
    failure = OSError("transfer")

    async def fail_drain():
        # Teardown cleared the registry and a new exchange reused identifier 1.
        futures[1] = later
        raise failure

    monkeypatch.setattr(client._effect_pump, "drain", fail_drain)
    with pytest.raises(OSError) as caught:
        await client._await_request_ack(future, futures, 1, 1, "SUBACK")
    assert caught.value is failure
    assert future.cancelled()
    assert futures == {1: later}
    assert not later.done()
    later.cancel()
