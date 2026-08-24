"""Lifecycle fallbacks when broker Maximum Packet Size forbids control packets."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.enums import ConnectionState, MQTTProtocolVersion
from mqttium.errors import MQTTError, ProtocolError
from mqttium.packets import AuthPacket
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.negotiated import NegotiatedSettings


class _Transport:
    def __init__(self) -> None:
        self.closed = False

    async def read(self, n: int = 65536) -> bytes:
        return b""

    async def write(self, data: bytes) -> None:
        pass

    async def close(self) -> None:
        self.closed = True

    def is_closing(self) -> bool:
        return self.closed


class _BlockingTransport(_Transport):
    def __init__(self) -> None:
        super().__init__()
        self._closed = asyncio.Event()

    async def read(self, n: int = 65536) -> bytes:
        await self._closed.wait()
        return b""

    async def close(self) -> None:
        self.closed = True
        self._closed.set()


async def test_disconnect_closes_transport_and_protocol_state_when_packet_cannot_fit() -> None:
    client = AsyncClient(protocol=MQTTProtocolVersion.MQTTv5)
    transport = _Transport()
    client._transport = transport
    client._engine.state = ConnectionState.CONNECTED
    client._engine.negotiated = NegotiatedSettings(maximum_packet_size=1)

    await client.disconnect()

    assert transport.closed
    assert client._transport is None
    assert client.state is ConnectionState.DISCONNECTED


async def test_disconnect_packet_size_fallback_still_reports_clean_callback() -> None:
    client = AsyncClient(protocol=MQTTProtocolVersion.MQTTv5)
    transport = _BlockingTransport()
    errors: list[BaseException | None] = []
    client._transport = transport
    client._engine.state = ConnectionState.CONNECTED
    client._engine.negotiated = NegotiatedSettings(maximum_packet_size=1)
    client.on_disconnect = errors.append
    client._reader_task = asyncio.create_task(client._read_loop())
    await asyncio.sleep(0)

    await client.disconnect()

    assert transport.closed
    assert client.state is ConnectionState.DISCONNECTED
    assert errors == [None]


async def test_missing_auth_handler_effect_failure_closes_active_connection() -> None:
    client = AsyncClient(
        protocol=MQTTProtocolVersion.MQTTv5,
        auth_handler=lambda challenge: challenge,
    )
    transport = _BlockingTransport()
    client._transport = transport
    client._engine.state = ConnectionState.CONNECTED
    client._engine.negotiated = NegotiatedSettings(maximum_packet_size=3)
    client.auth_handler = None
    client._reader_task = asyncio.create_task(client._read_loop())
    await asyncio.sleep(0)

    client._engine._emit(EffectKind.AUTH, AuthPacket(reason_code=0x18))
    client._collect_effects_locked()
    with pytest.raises(MQTTError, match="no longer available"):
        await client._drain_effects()
    reader = client._reader_task
    if reader is not None:
        await asyncio.wait_for(reader, timeout=1.0)

    assert transport.closed
    assert client.state is ConnectionState.DISCONNECTED
    assert isinstance(client._disconnect_exc, MQTTError)
    assert client._write_pump.queued_messages == 0


async def test_fatal_disconnect_is_not_queued_above_broker_packet_limit() -> None:
    client = AsyncClient(protocol=MQTTProtocolVersion.MQTTv5)
    transport = _Transport()
    client._transport = transport
    client._engine.state = ConnectionState.CONNECTED
    client._engine.negotiated = NegotiatedSettings(maximum_packet_size=3)

    await client._send_fatal_disconnect(ProtocolError("boom"))

    assert client._write_pump.queued_messages == 0
    assert client._write_pump.queued_bytes == 0
