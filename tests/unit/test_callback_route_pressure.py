"""Live callback routes retain FIFO and accounting under wire pressure."""

import asyncio
import functools
import pytest
from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion, QoS, ConnectionState
from mqttium.packets import PublishPacket
from tests.support import ScriptedBrokerTransport, transport_factory


def _assert_bounds(client, bound, budget):
    delivery = client.stats().delivery
    assert delivery.callback_queued <= bound
    if budget is not None:
        assert delivery.pending_high_water_bytes <= budget


@pytest.mark.parametrize("mode", ["callback", "auto", "both"])
@pytest.mark.parametrize("bound", [1, 2, 4])
@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
@pytest.mark.parametrize("qos", [0, 1])
@pytest.mark.parametrize("budget", [None, 4096])
async def test_reconfiguration_under_wire_queue_and_byte_pressure(
    mode, bound, protocol, qos, budget
):
    broker = ScriptedBrokerTransport(protocol=protocol)
    c = AsyncClient(
        client_id="pr454-adverse",
        protocol=protocol,
        message_delivery=mode,
        max_pending_callbacks=bound,
        max_pending_messages=2,
        max_pending_delivery_bytes=budget,
    )
    c._transport_factory = transport_factory(broker)
    seen = []
    iterated = []
    done = asyncio.Event()
    errors = []
    loop = asyncio.get_running_loop()
    prev = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))
    consumer = None

    def mark(m):
        assert not c._engine_lock.locked()
        _assert_bounds(c, bound, budget)
        i = int.from_bytes(m.payload[:2], "big")
        seen.append(i)
        if i == 31:
            done.set()
        return i

    async def later(m, marker=None):
        await asyncio.sleep(0)
        i = mark(m)
        if i == 10:
            c.on_message = functools.partial(later, marker="partial")
            c.message_callback_remove("pressure/x")
        if i == 20:
            c.message_callback_add("pressure/x", AsyncCallable())

    class AsyncCallable:
        async def __call__(self, m):
            await later(m)

    def first(m):
        mark(m)
        c.message_callback_add("pressure/x", later)

    c.message_callback_add("pressure/x", first)

    async def consume():
        async for m in c.messages():
            iterated.append(int.from_bytes(m.payload[:2], "big"))
            await asyncio.sleep(0)
            if len(iterated) == 32:
                return

    try:
        await c.connect("unused", 1883)
        if mode == "both":
            consumer = asyncio.create_task(consume())
        frames = b"".join(
            PublishPacket(
                topic="pressure/x",
                payload=i.to_bytes(2, "big") + b"x" * 510,
                qos=QoS(qos),
                mid=(i + 1) if qos else None,
                retain=False,
                dup=False,
            ).encode(protocol)
            for i in range(32)
        )
        broker.push_rx(frames)
        await asyncio.wait_for(done.wait(), 3)
        await asyncio.wait_for(c._callback_queue.join(), 3)
        if consumer is not None:
            await asyncio.wait_for(consumer, 3)
        # Deterministic writer barrier instead of sleeping a fixed number of turns.
        await asyncio.wait_for(c._flush_effects(), 3)
        assert seen == list(range(32))
        if mode == "both":
            assert iterated == seen
        assert errors == []
        assert c._callback_queue.maxsize == bound
        assert c.stats().delivery.callback_queued == 0
        assert c.stats().delivery.pending_bytes == 0
        assert c.state is ConnectionState.CONNECTED
    finally:
        if consumer is not None and not consumer.done():
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)
        await c.disconnect()
        loop.set_exception_handler(prev)


@pytest.mark.parametrize("self_cancel", [False, True])
@pytest.mark.parametrize("mode", ["callback", "both"])
async def test_sync_filtered_failure_remains_isolated_in_worker(self_cancel, mode):
    from collections import deque
    from mqttium.types import Message
    from mqttium.protocol.effects import EngineEffect, EffectKind

    c = AsyncClient(message_delivery=mode)
    seen = []
    errors = []
    loop = asyncio.get_running_loop()
    prev = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))
    sentinel = asyncio.CancelledError("user") if self_cancel else RuntimeError("user")

    def bad(m):
        raise sentinel

    def good(m):
        seen.append(m.payload)

    c.message_callback_add("a/#", bad)
    c.message_callback_add("a/+", good)
    try:
        effects = deque(
            EngineEffect(
                EffectKind.MESSAGE,
                Message(topic="a/x", payload=bytes([i])),
                requires_delivery_mark=False,
            )
            for i in range(3)
        )
        assert c._apply_message_effect_batch_inline(effects, c._connection_epoch) == 3
        await asyncio.wait_for(c._callback_queue.join(), 2)
        assert seen == [b"\0", b"\1", b"\2"]
        assert len(errors) == 3 and all(
            e["exception"] is sentinel and e["callback"] is bad for e in errors
        )
        assert c.stats().delivery.callback_queued == 0
    finally:
        await c._shutdown_callback_worker(drain=False)
        loop.set_exception_handler(prev)
