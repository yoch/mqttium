"""Invariant failures stay local, terminal, and distinct from store faults."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.api.models import PublishReceipt
from mqttium.codec.buffer import RawPacket
from mqttium.enums import ConnectionState, PacketType, QoS
from mqttium.packets import encode_frame
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.protocol.reconnect import ReconnectPolicy


def _raw_pingresp() -> RawPacket:
    return RawPacket(packet_type=PacketType.PINGRESP, flags=0, remaining=b"")


def _policy() -> ReconnectPolicy:
    return ReconnectPolicy(
        enabled=True,
        initial_delay=0.0,
        max_delay=0.0,
        stable_after=0.0,
        connect_timeout=0.1,
    )


class _ConnAckTransport:
    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._closed = False
        self._sent_connack = False

    async def write(self, data: bytes) -> None:
        if not self._sent_connack:
            self._sent_connack = True
            self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closed

    def feed(self, data: bytes) -> None:
        self._rx.put_nowait(data)


def test_engine_invariant_assertion_propagates_unchanged() -> None:
    engine = ProtocolEngine()
    engine.state = ConnectionState.CONNECTED
    failure = AssertionError("invariant exploded")

    def fail_handler(raw: RawPacket) -> None:
        raise failure

    engine._handlers_by_state[ConnectionState.CONNECTED][PacketType.PINGRESP] = fail_handler

    with pytest.raises(AssertionError) as raised:
        engine.handle_raw(_raw_pingresp())

    assert raised.value is failure
    assert engine.take_effects() == []


def test_engine_generic_handler_error_remains_contained() -> None:
    engine = ProtocolEngine()
    engine.state = ConnectionState.CONNECTED

    def fail_handler(raw: RawPacket) -> None:
        raise RuntimeError("store failed")

    engine._handlers_by_state[ConnectionState.CONNECTED][PacketType.PINGRESP] = fail_handler
    engine.handle_raw(_raw_pingresp())

    effects = engine.take_effects()
    assert len(effects) == 1
    assert effects[0].kind is EffectKind.PROTOCOL_ERROR
    assert "RuntimeError('store failed')" in str(effects[0].data)


async def test_runtime_invariant_failure_is_terminal_without_reconnect() -> None:
    client = AsyncClient(reconnect=_policy(), keepalive=0)
    transport = _ConnAckTransport()
    connect_calls = 0

    async def factory(
        host: str,
        port: int,
        *,
        ssl: object = None,
    ) -> _ConnAckTransport:
        nonlocal connect_calls
        connect_calls += 1
        return transport

    client._transport_factory = factory
    await client.connect("fake", timeout=0.2)

    failure = AssertionError("reader invariant")

    def fail_handler(raw: RawPacket) -> None:
        raise failure

    client._engine._handlers_by_state[ConnectionState.CONNECTED][PacketType.PINGRESP] = fail_handler
    receipt = PublishReceipt(mid=7, qos=QoS.AT_LEAST_ONCE)
    client._receipts[7] = receipt

    reader = client._reader_task
    assert reader is not None
    transport.feed(encode_frame(PacketType.PINGRESP, 0, b""))
    await asyncio.wait_for(reader, timeout=1.0)

    assert connect_calls == 1
    assert client._disconnect_exc is failure
    assert client._reconnect_task is None
    assert transport.is_closing()
    assert client._writer_task is None or client._writer_task.done()
    assert receipt.is_done()
    with pytest.raises(AssertionError, match="reader invariant"):
        await receipt.wait()


async def test_reconnect_loop_stops_if_reconnect_hits_invariant_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncClient(reconnect=_policy())
    client._host = "fake"
    client._port = 1883
    client._intentional_disconnect = False
    failure = AssertionError("reconnect invariant")
    connect_calls = 0

    async def fail_connect(*args: object, **kwargs: object) -> None:
        nonlocal connect_calls
        connect_calls += 1
        raise failure

    monkeypatch.setattr(client, "_connect_once_locked", fail_connect)
    receipt = PublishReceipt(mid=9, qos=QoS.AT_LEAST_ONCE)
    client._receipts[9] = receipt

    await asyncio.wait_for(client._reconnect_loop(), timeout=1.0)

    assert connect_calls == 1
    assert client._disconnect_exc is failure
    assert client._reconnect_task is None
    assert receipt.is_done()
    with pytest.raises(AssertionError, match="reconnect invariant"):
        await receipt.wait()
