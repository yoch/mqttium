"""Lifecycle fallbacks when broker Maximum Packet Size forbids control packets."""

from __future__ import annotations

from mqttium.api.async_client import AsyncClient
from mqttium.enums import ConnectionState, MQTTProtocolVersion
from mqttium.errors import ProtocolError
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


async def test_fatal_disconnect_is_not_queued_above_broker_packet_limit() -> None:
    client = AsyncClient(protocol=MQTTProtocolVersion.MQTTv5)
    transport = _Transport()
    client._transport = transport
    client._engine.state = ConnectionState.CONNECTED
    client._engine.negotiated = NegotiatedSettings(maximum_packet_size=3)

    await client._send_fatal_disconnect(ProtocolError("boom"))

    assert client._write_pump.queued_messages == 0
    assert client._write_pump.queued_bytes == 0
