"""Eager write exceptions retire the writer generation before returning to producers."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.api._effects import StaleConnectionEffect
from mqttium.api._writer import WritePump
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MQTTError
from mqttium.packets import PublishPacket
from tests.support import ScriptedBrokerTransport, transport_factory, wait_until


class _EagerPublishFailureBroker(ScriptedBrokerTransport):
    def __init__(self) -> None:
        super().__init__()
        self.failure = OSError("controlled eager write failure")
        self.eager_publish_attempts: list[bytes] = []
        self.awaited_publish_attempts = 0

    def write_nowait(self, data: bytes) -> bool:
        if data and data[0] & 0xF0 == int(PacketType.PUBLISH):
            self.eager_publish_attempts.append(bytes(data))
            raise self.failure
        return False

    async def write(self, data: bytes) -> None:
        if data and data[0] & 0xF0 == int(PacketType.PUBLISH):
            self.awaited_publish_attempts += 1
        await super().write(data)


@pytest.mark.parametrize("qos", [0, 1, 2])
@pytest.mark.parametrize("mode", ["async", "nowait"])
async def test_eager_publish_failure_retires_generation_without_wire_retry(
    qos: int,
    mode: str,
) -> None:
    broker = _EagerPublishFailureBroker()
    client = AsyncClient("eager-failure")
    client._transport_factory = transport_factory(broker)
    await client.connect("fake")
    await wait_until(lambda: client._write_pump._eager_armed)

    initial_epoch = client._write_pump.epoch
    try:
        with pytest.raises(OSError) as caught:
            if mode == "nowait":
                client.publish_nowait("eager/failure", b"x", qos=qos)
            else:
                await client.publish("eager/failure", b"x", qos=qos)
        assert caught.value is broker.failure

        # The producer sees the same exception, but the failed generation is
        # already fenced before another producer can use it.
        assert client._write_pump.epoch == initial_epoch + 1
        assert client._write_pump._write_nowait is None
        assert client._write_pump._latency_failure is broker.failure
        with pytest.raises(StaleConnectionEffect):
            client._write_pump.try_enqueue(b"must-not-use-failed-generation")

        await wait_until(lambda: client._transport is None)

        assert not client.is_connected
        assert client._disconnect_exc is broker.failure
        assert len(broker.eager_publish_attempts) == 1
        assert broker.awaited_publish_attempts == 0
        assert client._write_pump.queued_messages == 0
        assert client._write_pump.resident_messages == 0
        assert client._write_pump.queued_bytes == 0
        assert not client._receipts
    finally:
        await client.disconnect()


class _DirectEagerFailureTransport:
    def __init__(self) -> None:
        self.failure = OSError("direct eager failure")
        self.eager_calls: list[bytes] = []
        self.awaited_calls: list[bytes] = []

    def write_nowait(self, data: bytes) -> bool:
        self.eager_calls.append(data)
        raise self.failure

    async def write(self, data: bytes) -> None:
        self.awaited_calls.append(data)

    async def close(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False


async def test_eager_failure_is_fenced_before_control_returns_to_caller() -> None:
    failures: list[BaseException] = []
    reported = asyncio.Event()

    async def on_failure(exc: BaseException) -> None:
        failures.append(exc)
        reported.set()

    transport = _DirectEagerFailureTransport()
    pump = WritePump(max_bytes=1024, max_messages=4, on_failure=on_failure)
    pump.start(transport)
    initial_epoch = pump.epoch
    try:
        with pytest.raises(OSError) as caught:
            pump.try_enqueue(b"ambiguous")
        assert caught.value is transport.failure

        # No loop turn was required to fence the old generation.
        assert pump.epoch == initial_epoch + 1
        assert pump._write_nowait is None
        assert pump._latency_failure is transport.failure
        assert pump.queued_messages == pump.resident_messages == 1
        assert pump.queued_bytes == len(b"ambiguous")
        with pytest.raises(StaleConnectionEffect):
            pump.try_enqueue(b"late")

        await asyncio.wait_for(reported.wait(), timeout=1)
        await asyncio.wait_for(pump.join(), timeout=1)

        assert failures == [transport.failure]
        assert transport.eager_calls == [b"ambiguous"]
        assert transport.awaited_calls == []
        assert pump.queued_messages == pump.resident_messages == pump.queued_bytes == 0
    finally:
        await pump.stop()
        pump.discard()


class _EagerAckFailureBroker(ScriptedBrokerTransport):
    def __init__(self) -> None:
        super().__init__()
        self.failure = OSError("controlled eager PUBACK failure")
        self.eager_ack_attempts = 0
        self.awaited_ack_attempts = 0

    def write_nowait(self, data: bytes) -> bool:
        if data and data[0] & 0xF0 == int(PacketType.PUBACK):
            self.eager_ack_attempts += 1
            raise self.failure
        return False

    async def write(self, data: bytes) -> None:
        if data and data[0] & 0xF0 == int(PacketType.PUBACK):
            self.awaited_ack_attempts += 1
        await super().write(data)


async def test_eager_success_ack_failure_is_not_retried() -> None:
    broker = _EagerAckFailureBroker()
    client = AsyncClient("eager-ack-failure")
    client._transport_factory = transport_factory(broker)
    await client.connect("fake")
    await wait_until(lambda: client._write_pump._ack_eager_armed)

    broker.push_rx(
        PublishPacket(
            topic="incoming",
            payload=b"x",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            dup=False,
            mid=7,
        ).encode(MQTTProtocolVersion.MQTTv311)
    )

    try:
        await wait_until(lambda: not client.is_connected)
        assert client._disconnect_exc is broker.failure
        assert broker.eager_ack_attempts == 1
        assert broker.awaited_ack_attempts == 0
    finally:
        await client.disconnect()


class _CancelledEagerTransport(_DirectEagerFailureTransport):
    def write_nowait(self, data: bytes) -> bool:
        self.eager_calls.append(data)
        raise asyncio.CancelledError


async def test_eager_dependency_cancellation_fences_as_an_ordinary_failure() -> None:
    # A transport raising CancelledError that nobody requested is a failure of
    # that transport (#509): the producer gets an MQTTError caused by it, and
    # the generation is fenced exactly as for any other eager failure.
    failures: list[BaseException] = []
    reported = asyncio.Event()

    async def on_failure(exc: BaseException) -> None:
        failures.append(exc)
        reported.set()

    transport = _CancelledEagerTransport()
    pump = WritePump(max_bytes=1024, max_messages=4, on_failure=on_failure)
    pump.start(transport)
    initial_epoch = pump.epoch
    try:
        with pytest.raises(MQTTError) as caught:
            pump.try_enqueue_ack(b"ack")
        assert isinstance(caught.value.__cause__, asyncio.CancelledError)
        assert pump.epoch == initial_epoch + 1
        assert pump._latency_failure is caught.value

        await asyncio.wait_for(reported.wait(), timeout=1)
        assert failures == [caught.value]
        assert transport.awaited_calls == []
    finally:
        await pump.stop()
        pump.discard()
