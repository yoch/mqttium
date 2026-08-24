"""Adversarial delivery × reconnect × lifecycle compositions.

Stream-generation semantics from #343/#348 are the starting contract:

* automatic reconnect keeps the current ``messages()`` generation alive;
* a terminal disconnect ends that generation;
* a later explicit ``connect()`` starts a new generation.

These tests compose that contract with callbacks, both-mode accounting,
manual ACK, queue saturation, cancellation, and user callbacks that re-enter
the client. Failures here are local runtime bugs, not MQTT broker redelivery.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress

import pytest

from mqttium.api._delivery import ApplicationDelivery
from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import DEFAULT_MAX_PACKET_SIZE, IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import NotConnectedError, ProtocolError
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.types import Message


class _Broker:
    """Packet-aware broker with connect/read barriers for race tests."""

    def __init__(self, *, session_present: bool = False) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False
        self.connected = asyncio.Event()
        self.closed = asyncio.Event()
        self.session_present = session_present
        self.pubacks: list[int] = []
        self.pubcomps: list[int] = []

    async def write(self, data: bytes) -> None:
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                flags = 0x01 if self.session_present else 0x00
                self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, bytes((flags, 0x00))))
                self.connected.set()
            elif raw.packet_type is PacketType.PUBACK:
                self.pubacks.append(int.from_bytes(raw.remaining[:2], "big"))
            elif raw.packet_type is PacketType.PUBCOMP:
                self.pubcomps.append(int.from_bytes(raw.remaining[:2], "big"))

    async def read(self, n: int = 65536) -> bytes:
        del n
        return await self._rx.get()

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self._rx.put_nowait(b"")
        self.closed.set()

    def is_closing(self) -> bool:
        return self._closing

    def publish(
        self, topic: str, payload: bytes, *, qos: QoS = QoS.AT_MOST_ONCE, mid: int = 1
    ) -> None:
        self._rx.put_nowait(
            PublishPacket(
                topic=topic,
                payload=payload,
                qos=qos,
                retain=False,
                dup=False,
                mid=None if qos is QoS.AT_MOST_ONCE else mid,
            ).encode()
        )


def _policy(*, max_retries: int | None = 4, initial_delay: float = 0.0) -> ReconnectPolicy:
    return ReconnectPolicy(
        enabled=True,
        initial_delay=initial_delay,
        max_delay=max(initial_delay, 0.0),
        max_retries=max_retries,
        stable_after=0.0,
        connect_timeout=0.5,
    )


async def _cleanup(client: AsyncClient, *tasks: asyncio.Task[object] | None) -> None:
    for task in tasks:
        if task is None:
            continue
        if task.done():
            with suppress(asyncio.CancelledError, StopAsyncIteration, Exception):
                task.exception()
            continue
        task.cancel()
        with suppress(asyncio.CancelledError, StopAsyncIteration, Exception):
            await task
    with suppress(Exception, asyncio.CancelledError):
        await client.disconnect()


async def _wait_reconnect_gap(client: AsyncClient) -> None:
    for _ in range(400):
        if (
            client._reader_task is None or client._reader_task.done()
        ) and client._transport is None:
            if client._reconnect_task is not None and not client._reconnect_task.done():
                return
        await asyncio.sleep(0.01)
    raise AssertionError("client never entered a reconnect gap")


def _iterator_delivery() -> ApplicationDelivery:
    return ApplicationDelivery(
        mode="iterator",
        protocol=MQTTProtocolVersion.MQTTv311,
        max_pending_messages=8,
        max_pending_callbacks=1,
        max_pending_delivery_bytes=4096,
        maximum_packet_size=DEFAULT_MAX_PACKET_SIZE,
        delivery_timeout=1.0,
        callback_shutdown_timeout=1.0,
    )


async def _deliver(client: AsyncClient, payload: bytes, *, topic: str = "delivery/race") -> None:
    await client._apply_effect(
        EngineEffect(kind=EffectKind.MESSAGE, data=Message(topic=topic, payload=payload)),
        nowait=False,
    )


# --- stream generation contract under harder scheduling -------------------


async def test_suspended_iterator_receives_message_published_immediately_after_reconnect() -> None:
    brokers: list[_Broker] = []
    client = AsyncClient("imm-after-reconnect", reconnect=_policy(), message_delivery="iterator")

    async def factory(host: str, port: int, *, ssl=None):
        del host, port, ssl
        broker = _Broker()
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    pending: asyncio.Task[object] | None = None
    try:
        await client.connect("fake", 1, timeout=1.0)
        stream = client.messages()
        pending = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        await brokers[0].close()

        for _ in range(400):
            if client.is_connected and len(brokers) >= 2:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("reconnect did not complete")

        brokers[-1].publish("after", b"2")
        message = await asyncio.wait_for(pending, timeout=1.0)
        assert message.topic == "after"
        assert message.payload == b"2"
        assert not client._delivery.closed.is_set()
    finally:
        await _cleanup(client, pending)


async def test_suspended_iterator_does_not_consume_after_client_level_stream_reset() -> None:
    brokers: list[_Broker] = []
    client = AsyncClient("explicit-reset", message_delivery="iterator")

    async def factory(host: str, port: int, *, ssl=None):
        del host, port, ssl
        broker = _Broker()
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    old_pending: asyncio.Task[object] | None = None
    new_pending: asyncio.Task[object] | None = None
    try:
        await client.connect("fake", 1, timeout=1.0)
        old_stream = client.messages()
        old_pending = asyncio.create_task(anext(old_stream))
        await asyncio.sleep(0)
        await client.disconnect()
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(old_pending, timeout=1.0)

        await client.connect("fake", 1, timeout=1.0)
        brokers[-1].publish("new-gen", b"n")
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(old_stream), timeout=1.0)

        new_stream = client.messages()
        new_pending = asyncio.create_task(anext(new_stream))
        message = await asyncio.wait_for(new_pending, timeout=1.0)
        assert message.topic == "new-gen"
    finally:
        await _cleanup(client, old_pending, new_pending)


async def test_close_then_reset_before_waiter_runs_is_terminal_for_old_iterator() -> None:
    delivery = _iterator_delivery()
    old_stream = delivery.messages()
    pending = asyncio.create_task(anext(old_stream))
    await asyncio.sleep(0)

    delivery.close()
    delivery.reset_stream()
    await delivery.put_message(Message(topic="new", payload=b"x"))

    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(pending, timeout=1.0)
    assert await asyncio.wait_for(anext(delivery.messages()), timeout=1.0) == Message(
        topic="new", payload=b"x"
    )


# --- both-mode accounting -------------------------------------------------


async def test_both_mode_callback_release_does_not_drop_blocked_iterator_bytes() -> None:
    callback_started = asyncio.Event()
    callback_release = asyncio.Event()
    message = Message(topic="both/blocked", payload=b"payload-bytes")
    logical = len(message.topic) + len(message.payload)
    client = AsyncClient(
        message_delivery="both",
        max_pending_messages=1,
        max_pending_callbacks=1,
        max_pending_delivery_bytes=logical,
    )

    async def callback(received: Message) -> None:
        callback_started.set()
        await callback_release.wait()
        del received

    client.on_message = callback
    await client._apply_effect(EngineEffect(EffectKind.MESSAGE, message), nowait=False)
    await callback_started.wait()
    assert client.pending_delivery_bytes == logical

    blocked = asyncio.create_task(
        client._apply_effect(
            EngineEffect(
                EffectKind.MESSAGE, Message(topic="both/second", payload=b"payload-bytes")
            ),
            nowait=False,
        )
    )
    await asyncio.sleep(0)
    assert not blocked.done()

    callback_release.set()
    await asyncio.wait_for(client._callback_queue.join(), timeout=1.0)
    assert client.pending_delivery_bytes == logical
    assert not blocked.done()

    stream = client.messages()
    assert await anext(stream) is message
    await asyncio.wait_for(blocked, timeout=1.0)
    assert client._messages.qsize() == 1
    second = await anext(stream)
    assert second.topic == "both/second"
    await asyncio.wait_for(client._callback_queue.join(), timeout=1.0)
    assert client.pending_delivery_bytes == 0
    await stream.aclose()
    await client._shutdown_callback_worker(drain=False)
    blocked.cancel()
    with suppress(asyncio.CancelledError, Exception):
        await blocked


# --- callback failure / cancellation during reconnect ---------------------


async def test_callback_error_during_reconnect_does_not_terminate_stream() -> None:
    brokers: list[_Broker] = []
    received: list[bytes] = []
    fail_once = True
    client = AsyncClient(
        "cb-error-reconnect",
        reconnect=_policy(),
        message_delivery="callback",
    )

    def on_message(message: Message) -> None:
        nonlocal fail_once
        if fail_once:
            fail_once = False
            raise RuntimeError("callback boom")
        received.append(message.payload)

    client.on_message = on_message

    async def factory(host: str, port: int, *, ssl=None):
        del host, port, ssl
        broker = _Broker()
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    try:
        loop = asyncio.get_running_loop()
        recorded: list[dict[str, object]] = []
        loop.set_exception_handler(lambda _loop, context: recorded.append(context))
        await client.connect("fake", 1, timeout=1.0)
        brokers[0].publish("before", b"1")
        await asyncio.sleep(0.05)
        await brokers[0].close()
        for _ in range(400):
            if client.is_connected and len(brokers) >= 2:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("reconnect did not complete")
        brokers[-1].publish("after", b"2")
        for _ in range(200):
            if received == [b"2"]:
                break
            await asyncio.sleep(0.01)
        assert received == [b"2"]
        assert client._delivery.callback_task is not None
        assert not client._delivery.callback_task.done()
        assert any("callback boom" in str(item.get("exception")) for item in recorded)
    finally:
        asyncio.get_running_loop().set_exception_handler(None)
        await _cleanup(client)


async def test_callback_self_cancellation_does_not_stall_queued_jobs() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=8)
    seen: list[bytes] = []

    def on_message(message: Message) -> None:
        if message.payload == b"kill":
            raise asyncio.CancelledError
        seen.append(message.payload)

    client.on_message = on_message
    await _deliver(client, b"kill")
    await _deliver(client, b"keep")
    await _deliver(client, b"also")

    await asyncio.wait_for(client._callback_queue.join(), timeout=1.0)
    assert seen == [b"keep", b"also"]
    assert client._delivery.callback_task is not None
    assert not client._delivery.callback_task.done()
    await client._shutdown_callback_worker(drain=False)


# --- final shutdown accounting --------------------------------------------


async def test_final_shutdown_with_accounted_queue_and_no_consumer_releases_on_reset() -> None:
    client = AsyncClient(
        message_delivery="iterator",
        max_pending_delivery_bytes=1024,
    )
    await _deliver(client, b"abandoned-payload", topic="shutdown/left")
    assert client.pending_delivery_bytes > 0
    client._delivery.close()
    assert client.pending_delivery_bytes > 0

    await client._reset_message_stream()
    assert client.pending_delivery_bytes == 0
    assert client._messages.empty()


async def test_final_shutdown_does_not_deliver_to_closed_iterator() -> None:
    brokers: list[_Broker] = []
    client = AsyncClient("no-post-shutdown", message_delivery="iterator")

    async def factory(host: str, port: int, *, ssl=None):
        del host, port, ssl
        broker = _Broker()
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    pending: asyncio.Task[object] | None = None
    try:
        await client.connect("fake", 1, timeout=1.0)
        stream = client.messages()
        pending = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        await client.disconnect()
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(pending, timeout=1.0)
        brokers[-1].publish("late", b"nope")
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(stream), timeout=0.2)
        assert client._messages.empty() or client._delivery.closed.is_set()
    finally:
        await _cleanup(client, pending)


async def test_byte_waiters_are_released_when_the_abandoned_queue_is_reset() -> None:
    first = Message(topic="budget/first", payload=b"1234")
    logical = len(first.topic) + len(first.payload)
    client = AsyncClient(
        message_delivery="iterator",
        max_pending_messages=4,
        max_pending_delivery_bytes=logical,
    )
    await client._apply_effect(EngineEffect(EffectKind.MESSAGE, first), nowait=False)
    blocked = asyncio.create_task(
        client._apply_effect(
            EngineEffect(EffectKind.MESSAGE, Message(topic="budget/other", payload=b"x")),
            nowait=False,
        )
    )
    await asyncio.sleep(0)
    assert not blocked.done()
    assert client._delivery.waiters >= 1

    blocked.cancel()
    with suppress(asyncio.CancelledError):
        await blocked
    await client._reset_message_stream()

    assert client.pending_delivery_bytes == 0
    assert client._delivery.waiters == 0
    assert client._messages.empty()


async def test_disconnect_completes_while_reader_is_parked_on_delivery_bytes() -> None:
    brokers: list[_Broker] = []
    payload = b"1234"
    topic = "budget/first"
    logical = len(topic) + len(payload)
    client = AsyncClient(
        "budget-disconnect",
        message_delivery="iterator",
        max_pending_messages=4,
        max_pending_delivery_bytes=logical,
    )

    async def factory(host: str, port: int, *, ssl=None):
        del host, port, ssl
        broker = _Broker()
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    try:
        await client.connect("fake", 1, timeout=1.0)
        brokers[0].publish(topic, payload)
        for _ in range(50):
            if client.pending_delivery_bytes == logical:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("first accounted message was not delivered")
        brokers[0].publish("budget/other", b"x")
        for _ in range(50):
            if client._delivery.waiters:
                break
            await asyncio.sleep(0.01)
        await asyncio.wait_for(client.disconnect(), timeout=1.0)
        assert not client.is_connected
    finally:
        await _cleanup(client)


# --- manual ACK across transport loss -------------------------------------


async def test_manual_ack_during_reconnect_gap_is_not_connected() -> None:
    brokers: list[_Broker] = []
    attempts = 0
    client = AsyncClient(
        "manual-gap",
        reconnect=_policy(),
        message_delivery="iterator",
        manual_ack=True,
    )

    async def factory(host: str, port: int, *, ssl=None):
        nonlocal attempts
        del host, port, ssl
        attempts += 1
        if attempts > 1:
            await asyncio.Event().wait()
            raise OSError("unreachable")
        broker = _Broker()
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    pending: asyncio.Task[object] | None = None
    try:
        await client.connect("fake", 1, timeout=1.0)
        stream = client.messages()
        brokers[0].publish("need-ack", b"x", qos=QoS.AT_LEAST_ONCE, mid=9)
        message = await asyncio.wait_for(anext(stream), timeout=1.0)
        assert message.mid == 9
        assert brokers[0].pubacks == []

        await brokers[0].close()
        await _wait_reconnect_gap(client)

        with pytest.raises(NotConnectedError):
            await client.ack(message)
        pending = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        assert not pending.done()
    finally:
        await _cleanup(client, pending)


async def test_manual_ack_after_clean_reconnect_does_not_own_stale_mid() -> None:
    brokers: list[_Broker] = []
    client = AsyncClient(
        "manual-clean",
        reconnect=_policy(),
        message_delivery="iterator",
        manual_ack=True,
    )

    async def factory(host: str, port: int, *, ssl=None):
        del host, port, ssl
        broker = _Broker()
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    try:
        await client.connect("fake", 1, timeout=1.0)
        stream = client.messages()
        brokers[0].publish("need-ack", b"x", qos=QoS.AT_LEAST_ONCE, mid=4)
        message = await asyncio.wait_for(anext(stream), timeout=1.0)
        await brokers[0].close()
        for _ in range(400):
            if client.is_connected and len(brokers) >= 2:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("reconnect did not complete")
        with pytest.raises(ProtocolError):
            await client.ack(message)
        brokers[-1].publish("fresh", b"y", qos=QoS.AT_LEAST_ONCE, mid=4)
        fresh = await asyncio.wait_for(anext(stream), timeout=1.0)
        assert fresh.payload == b"y"
        await client.ack(fresh)
        for _ in range(200):
            if brokers[-1].pubacks == [4]:
                break
            await asyncio.sleep(0.01)
        assert brokers[-1].pubacks == [4]
    finally:
        await _cleanup(client)


# --- repeated reconnect while consumer stays active -----------------------


async def test_consumer_survives_repeated_reconnect_success_and_failure() -> None:
    brokers: list[_Broker] = []
    attempts = 0
    client = AsyncClient(
        "repeat-reconnect",
        reconnect=_policy(max_retries=8),
        message_delivery="iterator",
    )

    async def factory(host: str, port: int, *, ssl=None):
        nonlocal attempts
        del host, port, ssl
        attempts += 1
        if attempts in {2, 4}:
            raise OSError("transient factory failure")
        broker = _Broker()
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    pending: asyncio.Task[object] | None = None
    try:
        await client.connect("fake", 1, timeout=1.0)
        stream = client.messages()
        pending = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        await brokers[0].close()
        for _ in range(400):
            if client.is_connected and len(brokers) >= 2:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("first reconnect did not complete")
        await brokers[-1].close()
        for _ in range(400):
            if client.is_connected and len(brokers) >= 3:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("second reconnect did not complete")
        brokers[-1].publish("still-alive", b"ok")
        message = await asyncio.wait_for(pending, timeout=1.0)
        assert message.topic == "still-alive"
        assert attempts >= 5
    finally:
        await _cleanup(client, pending)


# --- queue saturation × reconnect -----------------------------------------


async def test_queue_saturation_then_consumer_resume_allows_reconnect() -> None:
    brokers: list[_Broker] = []
    client = AsyncClient(
        "sat-reconnect",
        reconnect=_policy(),
        message_delivery="iterator",
        max_pending_messages=1,
        delivery_timeout=2.0,
    )

    async def factory(host: str, port: int, *, ssl=None):
        del host, port, ssl
        broker = _Broker()
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    try:
        await client.connect("fake", 1, timeout=1.0)
        stream = client.messages()
        brokers[0].publish("first", b"1")
        first = await asyncio.wait_for(anext(stream), timeout=1.0)
        assert first.payload == b"1"

        brokers[0].publish("second", b"2")
        for _ in range(50):
            if client._messages.qsize() == 1:
                break
            await asyncio.sleep(0.01)
        brokers[0].publish("third", b"3")
        await asyncio.sleep(0)
        await brokers[0].close()

        second = await asyncio.wait_for(anext(stream), timeout=1.0)
        assert second.payload == b"2"
        for _ in range(400):
            if client.is_connected and len(brokers) >= 2:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("reconnect did not complete after saturation drained")
        brokers[-1].publish("after", b"4")
        later = await asyncio.wait_for(anext(stream), timeout=1.0)
        assert later.payload in {b"3", b"4"}
    finally:
        await _cleanup(client)


# --- cancellation vs generation change ------------------------------------


async def test_cancelling_suspended_iterator_during_reconnect_does_not_kill_generation() -> None:
    brokers: list[_Broker] = []
    client = AsyncClient("cancel-vs-reconnect", reconnect=_policy(), message_delivery="iterator")

    async def factory(host: str, port: int, *, ssl=None):
        del host, port, ssl
        broker = _Broker()
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    pending: asyncio.Task[object] | None = None
    try:
        await client.connect("fake", 1, timeout=1.0)
        stream = client.messages()
        pending = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        await brokers[0].close()
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        for _ in range(400):
            if client.is_connected and len(brokers) >= 2:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("reconnect did not complete")
        brokers[-1].publish("replacement", b"ok")
        # Cancelling anext() closes that async generator. The generation stays
        # alive across automatic reconnect; a new iterator resumes it.
        message = await asyncio.wait_for(anext(client.messages()), timeout=1.0)
        assert message.topic == "replacement"
    finally:
        await _cleanup(client, pending)


async def test_aclose_during_reconnect_does_not_close_generation_for_new_iterator() -> None:
    brokers: list[_Broker] = []
    client = AsyncClient("aclose-reconnect", reconnect=_policy(), message_delivery="iterator")

    async def factory(host: str, port: int, *, ssl=None):
        del host, port, ssl
        broker = _Broker()
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    try:
        await client.connect("fake", 1, timeout=1.0)
        old = client.messages()
        pending = asyncio.create_task(anext(old))
        await asyncio.sleep(0)
        await brokers[0].close()
        pending.cancel()
        with suppress(asyncio.CancelledError, StopAsyncIteration):
            await pending
        await old.aclose()
        for _ in range(400):
            if client.is_connected and len(brokers) >= 2:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("reconnect did not complete")
        brokers[-1].publish("next-iter", b"ok")
        message = await asyncio.wait_for(anext(client.messages()), timeout=1.0)
        assert message.topic == "next-iter"
    finally:
        await _cleanup(client)


# --- callbacks that re-enter the client -----------------------------------


async def test_on_disconnect_disconnect_stops_automatic_reconnect() -> None:
    brokers: list[_Broker] = []
    client = AsyncClient(
        "cb-disconnect",
        reconnect=_policy(max_retries=8),
        message_delivery="iterator",
    )

    async def on_disconnect(exc: BaseException | None) -> None:
        del exc
        await client.disconnect()

    client.on_disconnect = on_disconnect

    async def factory(host: str, port: int, *, ssl=None):
        del host, port, ssl
        broker = _Broker()
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    pending: asyncio.Task[object] | None = None
    try:
        await client.connect("fake", 1, timeout=1.0)
        stream = client.messages()
        pending = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        await brokers[0].close()
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(pending, timeout=1.0)
        await asyncio.sleep(0.05)
        assert client._reconnect_task is None or client._reconnect_task.done()
        assert not client.is_connected
        assert len(brokers) == 1
    finally:
        await _cleanup(client, pending)


async def test_on_disconnect_connect_does_not_lose_to_reconnect_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    brokers: list[_Broker] = []
    reconnect_sleep = asyncio.Event()
    release_reconnect_sleep = asyncio.Event()
    client = AsyncClient(
        "cb-connect",
        reconnect=_policy(initial_delay=1.0),
        message_delivery="iterator",
    )

    real_sleep = asyncio.sleep

    async def gated_sleep(delay: float) -> None:
        task = asyncio.current_task()
        if task is not None and task.get_name() == "mqttium-reconnect":
            reconnect_sleep.set()
            await release_reconnect_sleep.wait()
            await real_sleep(0)
            return
        await real_sleep(delay)

    monkeypatch.setattr("mqttium.api.async_client.asyncio.sleep", gated_sleep)

    async def on_disconnect(exc: BaseException | None) -> None:
        del exc
        if client._intentional_disconnect:
            return
        await client.connect("fake", 1, timeout=1.0)

    client.on_disconnect = on_disconnect

    async def factory(host: str, port: int, *, ssl=None):
        del host, port, ssl
        broker = _Broker()
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    pending: asyncio.Task[object] | None = None
    try:
        await client.connect("fake", 1, timeout=1.0)
        first = brokers[0]
        stream = client.messages()
        pending = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        await first.close()
        for _ in range(400):
            if client.is_connected and len(brokers) >= 2:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("on_disconnect connect did not establish a connection")
        generation = client._delivery._stream_generation
        release_reconnect_sleep.set()
        await asyncio.sleep(0.05)
        assert client.is_connected
        assert client._delivery._stream_generation == generation
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(pending, timeout=1.0)
        brokers[-1].publish("from-explicit", b"ok")
        message = await asyncio.wait_for(anext(client.messages()), timeout=1.0)
        assert message.payload == b"ok"
        assert client._reconnect_task is None or client._reconnect_task.done()
    finally:
        release_reconnect_sleep.set()
        await _cleanup(client, pending)


async def test_explicit_connect_during_reconnect_sleep_starts_new_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    brokers: list[_Broker] = []
    reconnect_sleep = asyncio.Event()
    release_reconnect_sleep = asyncio.Event()
    client = AsyncClient(
        "explicit-during-gap",
        reconnect=_policy(initial_delay=1.0),
        message_delivery="iterator",
    )

    real_sleep = asyncio.sleep

    async def gated_sleep(delay: float) -> None:
        task = asyncio.current_task()
        if task is not None and task.get_name() == "mqttium-reconnect":
            reconnect_sleep.set()
            await release_reconnect_sleep.wait()
            await real_sleep(0)
            return
        await real_sleep(delay)

    monkeypatch.setattr("mqttium.api.async_client.asyncio.sleep", gated_sleep)

    async def factory(host: str, port: int, *, ssl=None):
        del host, port, ssl
        broker = _Broker()
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    old_pending: asyncio.Task[object] | None = None
    try:
        await client.connect("fake", 1, timeout=1.0)
        old_stream = client.messages()
        old_pending = asyncio.create_task(anext(old_stream))
        await asyncio.sleep(0)
        await brokers[0].close()
        await asyncio.wait_for(reconnect_sleep.wait(), timeout=1.0)
        await client.connect("fake", 1, timeout=1.0)
        assert client.is_connected
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(old_pending, timeout=1.0)
        brokers[-1].publish("new-generation", b"n")
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(old_stream), timeout=0.2)
        message = await asyncio.wait_for(anext(client.messages()), timeout=1.0)
        assert message.topic == "new-generation"
        release_reconnect_sleep.set()
        await asyncio.sleep(0.2)
        assert client.is_connected
        assert not client._delivery.closed.is_set()
    finally:
        release_reconnect_sleep.set()
        await _cleanup(client, old_pending)


async def test_on_connect_disconnect_is_not_deadlocked() -> None:
    brokers: list[_Broker] = []
    client = AsyncClient("on-connect-disconnect", message_delivery="iterator")
    connected = asyncio.Event()

    async def on_connect(connack: object) -> None:
        del connack
        connected.set()
        await client.disconnect()

    client.on_connect = on_connect

    async def factory(host: str, port: int, *, ssl=None):
        del host, port, ssl
        broker = _Broker()
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    try:
        await asyncio.wait_for(client.connect("fake", 1, timeout=1.0), timeout=1.0)
        await asyncio.wait_for(connected.wait(), timeout=1.0)
        for _ in range(50):
            if not client.is_connected:
                break
            await asyncio.sleep(0.01)
        assert not client.is_connected
        worker = client._delivery.callback_task
        if worker is not None:
            await asyncio.wait_for(worker, timeout=1.0)
    finally:
        await _cleanup(client)


# --- shared-token reset while callback in flight --------------------------


async def test_reset_during_both_mode_inflight_callback_releases_exactly_once() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    message = Message(topic="both/reset", payload=b"payload")
    logical = len(message.topic) + len(message.payload)
    client = AsyncClient(
        message_delivery="both",
        max_pending_delivery_bytes=logical,
    )

    async def callback(received: Message) -> None:
        del received
        started.set()
        await release.wait()

    client.on_message = callback
    await client._apply_effect(EngineEffect(EffectKind.MESSAGE, message), nowait=False)
    await started.wait()
    assert client.pending_delivery_bytes == logical
    client._delivery.close()
    client._delivery.reset_stream()
    assert client._messages.empty()
    release.set()
    await asyncio.wait_for(client._callback_queue.join(), timeout=1.0)
    assert client.pending_delivery_bytes == 0
    await client._shutdown_callback_worker(drain=False)
