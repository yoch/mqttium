"""Regression coverage for live topic-route reconfiguration (issue #453)."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Iterator
from contextlib import contextmanager

import pytest

from mqttium.api import AsyncClient
from mqttium.api._delivery import MessageDelivery
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message
from tests.support import ScriptedBrokerTransport, transport_factory


@contextmanager
def _callback_errors() -> Iterator[list[dict[str, object]]]:
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    errors: list[dict[str, object]] = []
    loop.set_exception_handler(lambda _loop, context: errors.append(context))
    try:
        yield errors
    finally:
        loop.set_exception_handler(previous)


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
@pytest.mark.parametrize("mode", ["callback", "auto", "both"])
@pytest.mark.parametrize("burst", [2, 3])
async def test_qos1_reconfiguration_delivers_every_acknowledged_message(
    protocol: MQTTProtocolVersion, mode: MessageDelivery, burst: int
) -> None:
    """Reconfiguration inside the first handler must not drop the batch tail."""
    transport = ScriptedBrokerTransport(protocol=protocol)
    client = AsyncClient(client_id="rc14-audit", protocol=protocol, message_delivery=mode)
    client._transport_factory = transport_factory(transport)
    seen: list[str] = []
    first_seen = asyncio.Event()

    async def replacement(message: Message) -> None:
        seen.append("new:" + message.payload.decode())

    def original(message: Message) -> None:
        seen.append("old:" + message.payload.decode())
        if message.payload == b"1":
            client.message_callback_add("audit/x", replacement)
            first_seen.set()

    client.message_callback_add("audit/x", original)
    with _callback_errors() as errors:
        try:
            await asyncio.wait_for(client.connect("unused", 1883), timeout=2)
            transport.push_rx(
                b"".join(
                    PublishPacket(
                        topic="audit/x",
                        payload=str(mid).encode(),
                        qos=QoS.AT_LEAST_ONCE,
                        retain=False,
                        dup=False,
                        mid=mid,
                    ).encode(protocol)
                    for mid in range(1, burst + 1)
                )
            )
            await asyncio.wait_for(first_seen.wait(), timeout=2)
            await asyncio.wait_for(client._callback_queue.join(), timeout=2)
            for _ in range(20):
                await asyncio.sleep(0)
            decoder = IncrementalDecoder()
            decoder.feed(b"".join(transport.written))
            acknowledged = [
                int.from_bytes(raw.remaining[:2], "big")
                for raw in decoder.drain_packets()
                if raw.packet_type is PacketType.PUBACK
            ]
            assert acknowledged == list(range(1, burst + 1))
            assert client.state is ConnectionState.CONNECTED
            assert seen == ["old:1"] + [f"new:{mid}" for mid in range(2, burst + 1)]
            assert errors == []
        finally:
            await asyncio.wait_for(client.disconnect(), timeout=2)


@pytest.mark.parametrize("mutation", ["replace_filter", "change_fallback", "remove_last_filter"])
@pytest.mark.parametrize("kind", [EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE])
async def test_queued_router_survives_sync_to_async_reconfiguration(
    mutation: str, kind: EffectKind
) -> None:
    """An already captured internal router must not misclassify a valid async def."""
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []

    def original(message: Message) -> None:
        seen.append("old:" + message.payload.decode())

    async def replacement(message: Message) -> None:
        seen.append("new:" + message.payload.decode())

    client.on_message = original
    client.message_callback_add("other/#" if mutation == "change_fallback" else "audit/x", original)
    effects = deque(
        EngineEffect(
            kind,
            Message(topic="audit/x", payload=str(i).encode()),
            requires_delivery_mark=False,
            decoded_property_wire_size=0 if kind is EffectKind.DECODED_MESSAGE else None,
        )
        for i in range(3)
    )
    with _callback_errors() as errors:
        try:
            assert client._apply_message_effect_batch_inline(effects, client._connection_epoch) == 3
            assert seen == []  # Prove the burst is queued, not already delivered.
            if mutation == "replace_filter":
                client.message_callback_add("audit/x", replacement)
            else:
                client.on_message = replacement
                if mutation == "remove_last_filter":
                    client.message_callback_remove("audit/x")
            await asyncio.wait_for(client._callback_queue.join(), timeout=2)
            assert seen == ["new:0", "new:1", "new:2"]
            assert errors == []
        finally:
            await client._shutdown_callback_worker(drain=False)


def _messages(count: int, kind: EffectKind) -> deque[EngineEffect]:
    return deque(
        EngineEffect(
            kind,
            Message(topic="audit/x", payload=str(i).encode()),
            requires_delivery_mark=False,
            decoded_property_wire_size=0 if kind is EffectKind.DECODED_MESSAGE else None,
        )
        for i in range(count)
    )


@pytest.mark.parametrize("burst", [2, 3])
@pytest.mark.parametrize("kind", [EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE])
async def test_reconfigured_tail_keeps_fifo_and_hard_callback_bound(
    burst: int, kind: EffectKind
) -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=burst)
    seen: list[str] = []
    entered = asyncio.Event()
    release = asyncio.Event()

    async def replacement(message: Message) -> None:
        assert not client._engine_lock.locked()
        value = message.payload.decode()
        seen.append(f"start:{value}")
        if value == "1":
            entered.set()
            await release.wait()
        seen.append(f"end:{value}")

    def original(message: Message) -> None:
        assert not client._engine_lock.locked()
        seen.append("old:" + message.payload.decode())
        client.message_callback_add("audit/x", replacement)
        assert client._delivery.try_enqueue_callback(lambda: seen.append("reentrant"))
        assert client.stats().delivery.callback_queued == burst
        assert not client._delivery.try_enqueue_callback(lambda: seen.append("overflow"))

    client.message_callback_add("audit/x", original)
    with _callback_errors() as errors:
        try:
            assert (
                client._apply_message_effect_batch_inline(
                    _messages(burst, kind), client._connection_epoch
                )
                == burst
            )
            await asyncio.wait_for(entered.wait(), timeout=2)
            assert seen == ["old:0", "start:1"]
            # The ordinary queue reports only unstarted notifications; the
            # currently active callback is deliberately excluded from qsize.
            expected_pending = burst - 1
            assert client.stats().delivery.callback_queued == expected_pending
            # Exactly one queue slot is free because the active callback is not queued.
            assert client._delivery.try_enqueue_callback(lambda: seen.append("refill"))
            assert client.stats().delivery.callback_queued == burst
            assert not client._delivery.try_enqueue_callback(lambda: seen.append("overflow"))
            release.set()
            await asyncio.wait_for(client._callback_queue.join(), timeout=2)
            expected = ["old:0"]
            for i in range(1, burst):
                expected.extend([f"start:{i}", f"end:{i}"])
            assert seen == expected + ["reentrant", "refill"]
            assert client.stats().delivery.callback_queued == 0
            assert client._callback_queue.maxsize == burst
            assert errors == []
        finally:
            release.set()
            await client._shutdown_callback_worker(drain=False)


@pytest.mark.parametrize("mutation", ["replace_filter", "remove_all_filters"])
@pytest.mark.parametrize("kind", [EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE])
async def test_reconfiguration_during_await_keeps_only_current_message_snapshot(
    mutation: str, kind: EffectKind
) -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []
    entered = asyncio.Event()
    release = asyncio.Event()

    async def first(message: Message) -> None:
        value = message.payload.decode()
        seen.append("first:" + value)
        if value == "0":
            entered.set()
            await release.wait()

    def old_tail(message: Message) -> None:
        seen.append("old:" + message.payload.decode())

    async def new_tail(message: Message) -> None:
        await asyncio.sleep(0)
        seen.append("new:" + message.payload.decode())

    client.message_callback_add("audit/#", first)
    client.message_callback_add("audit/+", old_tail)
    with _callback_errors() as errors:
        try:
            assert (
                client._apply_message_effect_batch_inline(
                    _messages(2, kind), client._connection_epoch
                )
                == 2
            )
            await asyncio.wait_for(entered.wait(), timeout=2)
            if mutation == "replace_filter":
                client.message_callback_add("audit/+", new_tail)
            else:
                client.on_message = new_tail
                client.message_callback_remove("audit/#")
                client.message_callback_remove("audit/+")
            release.set()
            await asyncio.wait_for(client._callback_queue.join(), timeout=2)
            expected = ["first:0", "old:0"]
            if mutation == "replace_filter":
                expected.append("first:1")
            assert seen == expected + ["new:1"]
            assert errors == []
        finally:
            release.set()
            await client._shutdown_callback_worker(drain=False)


@pytest.mark.parametrize("kind", [EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE])
async def test_reconfigured_async_tail_keeps_worker_cancellation_ownership(
    kind: EffectKind,
) -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=3)
    seen: list[str] = []
    entered = asyncio.Event()
    release = asyncio.Event()
    cancelled = asyncio.Event()

    async def replacement(message: Message) -> None:
        seen.append("new:" + message.payload.decode())
        entered.set()
        try:
            await release.wait()
        finally:
            cancelled.set()

    def original(message: Message) -> None:
        seen.append("old:" + message.payload.decode())
        client.message_callback_add("audit/x", replacement)

    client.message_callback_add("audit/x", original)
    with _callback_errors() as errors:
        try:
            assert (
                client._apply_message_effect_batch_inline(
                    _messages(3, kind), client._connection_epoch
                )
                == 3
            )
            await asyncio.wait_for(entered.wait(), timeout=2)
            task = client._callback_worker_task
            assert task is not None
            await asyncio.wait_for(client._shutdown_callback_worker(drain=False), timeout=2)
            assert task.cancelled()
            assert cancelled.is_set()
            assert seen == ["old:0", "new:1"]
            assert client.stats().delivery.callback_queued == 0
            assert client._callback_queue.maxsize == 3
            assert errors == []
        finally:
            release.set()
            await client._shutdown_callback_worker(drain=False)
