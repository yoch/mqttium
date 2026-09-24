"""CancelledError raised by a dependency is a failure, not an owner cancellation.

Every runtime boundary decides cancellation ownership from the task's pending
cancel requests (``mqttium.api._cancel``). These tests replay one issue each:
a dependency raises ``CancelledError`` while nobody cancelled the boundary's
task, and a negative control cancels the owner for real.
"""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient, PublishMessage
from mqttium.api._cancel import (
    DependencyCancelledError,
    dependency_failure,
    failure_for,
    ignoring_dependency_failures,
    owner_cancelled,
)
from mqttium.api._writer import WritePump
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MQTTError, PublishBatchError
from mqttium.packets import encode_frame
from mqttium.protocol.reconnect import ReconnectPolicy
from tests.support import ScriptedBrokerTransport, transport_factory, wait_until


def _no_retry_policy(max_retries: int) -> ReconnectPolicy:
    return ReconnectPolicy(
        initial_delay=0.0,
        multiplier=1.0,
        max_delay=0.0,
        max_retries=max_retries,
        stable_after=0.0,
    )


def _assert_dependency_failure(error: BaseException | None, cause: BaseException) -> None:
    assert isinstance(error, DependencyCancelledError)
    assert isinstance(error, MQTTError)
    assert error.__cause__ is cause


# --- the primitive ----------------------------------------------------------


async def test_owner_cancelled_tracks_pending_cancel_requests() -> None:
    assert not owner_cancelled()
    started = asyncio.Event()

    async def child() -> None:
        started.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(child())
    await started.wait()
    assert not owner_cancelled(task)
    task.cancel()
    assert owner_cancelled(task)
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_failure_for_converts_only_unowned_cancellation() -> None:
    error = OSError("unchanged")
    assert failure_for(error, "x") is error
    cancelled = asyncio.CancelledError("dependency")
    _assert_dependency_failure(failure_for(cancelled, "x"), cancelled)
    assert str(dependency_failure(cancelled, "x", "custom")) == "custom"


async def test_ignoring_dependency_failures_keeps_owner_cancellation() -> None:
    with ignoring_dependency_failures():
        raise asyncio.CancelledError("dependency")
    with ignoring_dependency_failures():
        raise OSError("teardown")

    entered = asyncio.Event()

    async def owner() -> None:
        with ignoring_dependency_failures():
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(owner())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# --- #509: WritePump --------------------------------------------------------


class _CancelledPublishTransport(ScriptedBrokerTransport):
    def __init__(self) -> None:
        super().__init__()
        self.failure = asyncio.CancelledError("transport self-cancel")
        self.publish_attempts = 0

    async def write(self, data: bytes) -> None:
        if data and data[0] & 0xF0 == int(PacketType.PUBLISH):
            self.publish_attempts += 1
            raise self.failure
        await super().write(data)

    async def write_many(self, parts: list[bytes]) -> None:
        await self.write(b"".join(parts))


async def test_transport_cancelled_error_retires_writer_generation_and_receipt() -> None:
    transport = _CancelledPublishTransport()
    client = AsyncClient("writer-dependency-cancel")
    client._transport_factory = transport_factory(transport)
    await client.connect("fake")
    try:
        receipt = await client.publish("writer/cancel", b"x", qos=QoS.AT_LEAST_ONCE)
        await wait_until(lambda: not client.is_connected)
        await wait_until(receipt.is_done)

        assert transport.publish_attempts == 1
        _assert_dependency_failure(client._disconnect_exc, transport.failure)
        assert not client._receipts
        assert client._write_pump.epoch == client._connection_epoch
        with pytest.raises(DependencyCancelledError) as caught:
            await receipt.wait()
        assert caught.value.__cause__ is transport.failure
    finally:
        await client.disconnect()


async def test_writer_task_cancellation_remains_lifecycle_owned() -> None:
    failures: list[BaseException] = []
    entered = asyncio.Event()

    async def on_failure(exc: BaseException) -> None:
        failures.append(exc)

    class _Blocking:
        async def write(self, data: bytes) -> None:
            del data
            entered.set()
            await asyncio.Event().wait()

        async def write_many(self, parts: list[bytes]) -> None:
            await self.write(b"".join(parts))

        async def close(self) -> None:
            return None

        def is_closing(self) -> bool:
            return False

    pump = WritePump(max_bytes=1024, max_messages=2, on_failure=on_failure)
    pump.start(_Blocking())
    pump._eager_armed = False
    assert pump.try_enqueue(b"x")
    await asyncio.wait_for(entered.wait(), timeout=1)

    await pump.stop()

    assert failures == []
    assert pump.task is None


# --- #510: reconnect supervisor --------------------------------------------


async def test_dependency_cancelled_error_uses_reconnect_retry_and_terminal_policy() -> None:
    first = ScriptedBrokerTransport()
    failure = asyncio.CancelledError("factory self-cancel")
    calls = 0
    client = AsyncClient(
        "reconnect-dependency-cancel",
        keepalive=0,
        reconnect=_no_retry_policy(2),
        connect_timeout=0.1,
    )

    async def factory(host: str, port: int, *, ssl: object = None) -> ScriptedBrokerTransport:
        nonlocal calls
        del host, port, ssl
        calls += 1
        if calls == 1:
            return first
        raise failure

    client._transport_factory = factory
    await client.connect("fake")
    stream = client.messages()
    try:
        first.push_rx(b"")
        await wait_until(lambda: client._delivery.closed.is_set())
        await wait_until(lambda: client._reconnect_task is None)

        assert calls == 3  # initial connection + two retry attempts
        assert not client.is_connected
        _assert_dependency_failure(client._disconnect_exc, failure)
        assert client.stats().reconnect_attempt == 2
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(stream), timeout=1)
    finally:
        await stream.aclose()
        await client.disconnect()


async def test_disconnect_cancels_in_progress_reconnect_without_extra_retry() -> None:
    first = ScriptedBrokerTransport()
    reconnect_entered = asyncio.Event()
    calls = 0
    client = AsyncClient(
        "reconnect-owner-cancel",
        keepalive=0,
        reconnect=_no_retry_policy(5),
        connect_timeout=30.0,
    )

    async def factory(host: str, port: int, *, ssl: object = None) -> ScriptedBrokerTransport:
        nonlocal calls
        del host, port, ssl
        calls += 1
        if calls == 1:
            return first
        reconnect_entered.set()
        await asyncio.Event().wait()
        raise AssertionError("blocked reconnect factory resumed without cancellation")

    client._transport_factory = factory
    await client.connect("fake")
    try:
        first.push_rx(b"")
        await asyncio.wait_for(reconnect_entered.wait(), timeout=1)
        await asyncio.wait_for(client.disconnect(), timeout=1)
        for _ in range(4):
            await asyncio.sleep(0)

        assert calls == 2
        assert client._reconnect_task is None
        assert client._delivery.closed.is_set()
    finally:
        await client.disconnect()


# --- #522: publish_many -----------------------------------------------------


async def test_publish_many_reports_dependency_cancellation_with_prefix_receipt() -> None:
    transport = ScriptedBrokerTransport()
    client = AsyncClient("batch-dependency-cancel", keepalive=0)
    client._transport_factory = transport_factory(transport)
    failure = asyncio.CancelledError("source self-cancel")

    def source():
        yield PublishMessage("batch/a", b"1", qos=QoS.AT_LEAST_ONCE)
        raise failure

    await client.connect("fake")
    try:
        with pytest.raises(PublishBatchError) as caught:
            await client.publish_many(source())
        _assert_dependency_failure(caught.value.__cause__, failure)
        receipt = caught.value.receipt
        assert receipt is not None
        assert receipt.submitted == 1
        await asyncio.wait_for(receipt.wait(), timeout=1)
    finally:
        await client.disconnect()


async def test_publish_many_caller_cancellation_propagates_unchanged() -> None:
    transport = ScriptedBrokerTransport()
    client = AsyncClient("batch-owner-cancel", keepalive=0)
    client._transport_factory = transport_factory(transport)
    started = asyncio.Event()

    async def run() -> None:
        def source():
            yield PublishMessage("batch/a", b"1", qos=QoS.AT_LEAST_ONCE)
            started.set()
            yield PublishMessage("batch/b", b"2", qos=QoS.AT_LEAST_ONCE)

        await client.publish_many(source())
        await asyncio.Event().wait()

    await client.connect("fake")
    try:
        task = asyncio.create_task(run())
        await asyncio.wait_for(started.wait(), timeout=1)
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        await client.disconnect()


# --- #525: EffectPump --------------------------------------------------------


class _CancellingCloseTransport(ScriptedBrokerTransport):
    def __init__(self) -> None:
        super().__init__(protocol=MQTTProtocolVersion.MQTTv5)
        self.failure = asyncio.CancelledError("close self-cancel")
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1
        await super().close()
        if self.close_calls == 1:
            raise self.failure


async def test_transport_close_cancelled_error_does_not_kill_effect_pump() -> None:
    transport = _CancellingCloseTransport()
    disconnects: list[BaseException | None] = []
    client = AsyncClient("pump-dependency-cancel", protocol=MQTTProtocolVersion.MQTTv5)
    client.on_disconnect = disconnects.append
    client._transport_factory = transport_factory(transport)
    await client.connect("fake")
    try:
        # Broker DISCONNECT (0x8B server shutting down): applying it closes the
        # transport, whose close() raises CancelledError on its own.
        transport.push_rx(encode_frame(PacketType.DISCONNECT, 0, b"\x8b\x00"))
        await wait_until(lambda: disconnects != [])
        assert transport.close_calls >= 1
        assert not client.is_connected
        assert "0x8b" in str(disconnects[0]).lower() or "139" in str(disconnects[0])
        await asyncio.wait_for(client.disconnect(), timeout=1)
    finally:
        await client.disconnect()


# --- #529: explicit connect -------------------------------------------------


async def test_connect_reports_transport_factory_cancellation_as_failure() -> None:
    failure = asyncio.CancelledError("factory self-cancel")

    async def factory(host: str, port: int, *, ssl: object = None) -> ScriptedBrokerTransport:
        del host, port, ssl
        raise failure

    client = AsyncClient("connect-dependency-cancel")
    client._transport_factory = factory
    with pytest.raises(DependencyCancelledError) as caught:
        await client.connect("fake")
    assert caught.value.__cause__ is failure
    assert not client.is_connected


async def test_connect_caller_cancellation_propagates_unchanged() -> None:
    entered = asyncio.Event()

    async def factory(host: str, port: int, *, ssl: object = None) -> ScriptedBrokerTransport:
        del host, port, ssl
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    client = AsyncClient("connect-owner-cancel")
    client._transport_factory = factory
    task = asyncio.create_task(client.connect("fake"))
    await asyncio.wait_for(entered.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# --- #538: reader -----------------------------------------------------------


class _CancelledReadTransport(ScriptedBrokerTransport):
    def __init__(self) -> None:
        super().__init__()
        self.failure = asyncio.CancelledError("read self-cancel")
        self.armed = False

    async def read(self, n: int = 65536) -> bytes:
        data = await super().read(n)
        if self.armed and data == b"":
            raise self.failure
        return data


async def test_transport_read_cancellation_keeps_its_cause() -> None:
    transport = _CancelledReadTransport()
    disconnects: list[BaseException | None] = []
    client = AsyncClient("reader-dependency-cancel", keepalive=0)
    client.on_disconnect = disconnects.append
    client._transport_factory = transport_factory(transport)
    await client.connect("fake")
    try:
        transport.armed = True
        transport.push_rx(b"")
        await wait_until(lambda: disconnects != [])
        _assert_dependency_failure(disconnects[0], transport.failure)
        _assert_dependency_failure(client._disconnect_exc, transport.failure)
    finally:
        await client.disconnect()


# --- keepalive --------------------------------------------------------------


async def test_keepalive_dependency_cancellation_retires_connection() -> None:
    transport = ScriptedBrokerTransport()
    disconnects: list[BaseException | None] = []
    client = AsyncClient("keepalive-dependency-cancel", keepalive=1)
    client.on_disconnect = disconnects.append
    client._transport_factory = transport_factory(transport)
    failure = asyncio.CancelledError("ping self-cancel")

    def queue_ping() -> None:
        raise failure

    await client.connect("fake")
    try:
        client._engine.queue_ping = queue_ping  # type: ignore[method-assign]
        client._write_pump.last_outbound = 0.0
        await wait_until(lambda: disconnects != [])
        _assert_dependency_failure(disconnects[0], failure)
    finally:
        await client.disconnect()
