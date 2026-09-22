"""Cancelled callers do not retain futures or abandon protocol identifiers."""

import asyncio
import gc

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.packets import encode_frame
from tests.support import ScriptedBrokerTransport, transport_factory, wait_until


@pytest.mark.parametrize("unsubscribe", [False, True])
@pytest.mark.parametrize("during_transfer", [False, True])
async def test_cancelled_request_keeps_protocol_owner_only(unsubscribe, during_transfer):
    class DelayedAckTransport(ScriptedBrokerTransport):
        def __init__(self):
            super().__init__(protocol=MQTTProtocolVersion.MQTTv5)
            self.gate = asyncio.Event()
            self.gate.set()
            self.held = asyncio.Event()
            self.request_seen = asyncio.Event()

        async def write(self, data):
            if not self.gate.is_set():
                self.held.set()
            await self.gate.wait()
            await super().write(data)

        def handle_packet(self, raw):
            if raw.packet_type in (PacketType.SUBSCRIBE, PacketType.UNSUBSCRIBE):
                self.request_seen.set()
            else:
                super().handle_packet(raw)

        async def close(self):
            self.gate.set()
            await super().close()

    transport = DelayedAckTransport()
    client = AsyncClient(
        "request-cancel",
        protocol=MQTTProtocolVersion.MQTTv5,
        keepalive=0,
        max_write_queue_messages=1,
    )
    client._transport_factory = transport_factory(transport)
    await client.connect("unused")
    request = None
    try:
        if during_transfer:
            transport.gate.clear()
            await client.publish("hold", b"x")
            await transport.held.wait()
        request = asyncio.create_task(
            (client.unsubscribe if unsubscribe else client.subscribe)("t")
        )
        if during_transfer:
            await wait_until(lambda: client._write_pump.waiters > 0)
        else:
            await asyncio.wait_for(transport.request_seen.wait(), 1)
        futures = client._unsub_futs if unsubscribe else client._sub_futs
        [(mid, future)] = futures.items()
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
        assert future.cancelled()
        assert not futures
        assert client._engine.outbound.packet_ids.in_use(mid)
        transport.gate.set()
        await asyncio.wait_for(transport.request_seen.wait(), 1)
        kind = PacketType.UNSUBACK if unsubscribe else PacketType.SUBACK
        transport.push_rx(encode_frame(kind, 0, mid.to_bytes(2, "big") + b"\x00\x00"))
        await wait_until(lambda: not client._engine.outbound.packet_ids.in_use(mid))
        assert client.is_connected
    finally:
        transport.gate.set()
        if request is not None:
            request.cancel()
            await asyncio.gather(request, return_exceptions=True)
        await client.disconnect()


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
