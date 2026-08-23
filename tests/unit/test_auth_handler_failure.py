"""Regression coverage for enhanced-auth handler failures during CONNECT."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.errors import MQTTError
from mqttium.packets import AuthPacket, encode_frame
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.types import Properties


class _AuthChallengeTransport:
    def __init__(self, *, challenge_during_connect: bool = True) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False
        self._challenge_during_connect = challenge_during_connect

    async def write(self, data: bytes | tuple[bytes, bytes]) -> None:
        wire = data if isinstance(data, bytes) else data[0] + data[1]
        self._decoder.feed(wire)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                if self._challenge_during_connect:
                    self.challenge()
                else:
                    props = Properties({"authentication_method": "demo"})
                    body = b"\x00\x00" + encode_properties(props, "CONNACK")
                    self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, body))

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        self._closing = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing

    def challenge(self) -> None:
        props = Properties({"authentication_method": "demo"})
        self._rx.put_nowait(
            AuthPacket(reason_code=0x18, properties=props).encode(MQTTProtocolVersion.MQTTv5)
        )


async def test_auth_handler_exception_propagates_from_connect() -> None:
    transport = _AuthChallengeTransport()

    async def bad_auth(_packet: AuthPacket) -> None:
        raise RuntimeError("auth boom")

    props = Properties()
    props.set("authentication_method", "demo")
    client = AsyncClient(
        client_id="auth-failure",
        protocol=MQTTProtocolVersion.MQTTv5,
        connect_properties=props,
        auth_handler=bad_auth,
    )

    async def factory(
        host: str,
        port: int,
        *,
        ssl: object = None,
    ) -> _AuthChallengeTransport:
        return transport

    client._transport_factory = factory

    with pytest.raises(RuntimeError, match="auth boom"):
        await client.connect("fake", timeout=0.5)

    assert transport.is_closing()
    assert client.state is ConnectionState.DISCONNECTED
    assert client._transport is None


async def test_self_cancelled_reauth_handler_is_a_reconnectable_failure() -> None:
    transports: list[_AuthChallengeTransport] = []
    disconnected = asyncio.Event()
    disconnect_errors: list[BaseException | None] = []

    async def cancel_auth(_packet: AuthPacket) -> None:
        raise asyncio.CancelledError("user AUTH cancellation")

    props = Properties({"authentication_method": "demo"})
    client = AsyncClient(
        client_id="auth-cancellation",
        protocol=MQTTProtocolVersion.MQTTv5,
        connect_properties=props,
        auth_handler=cancel_auth,
        reconnect=ReconnectPolicy(
            enabled=True,
            initial_delay=0,
            max_delay=0,
            max_retries=2,
            stable_after=0,
            connect_timeout=0.2,
        ),
    )

    def on_disconnect(error: BaseException | None) -> None:
        disconnect_errors.append(error)
        disconnected.set()

    client.on_disconnect = on_disconnect

    async def factory(
        host: str,
        port: int,
        *,
        ssl: object = None,
    ) -> _AuthChallengeTransport:
        transport = _AuthChallengeTransport(challenge_during_connect=False)
        transports.append(transport)
        return transport

    client._transport_factory = factory
    try:
        await client.connect("fake", timeout=0.5)
        await client.auth()
        transports[0].challenge()

        await asyncio.wait_for(disconnected.wait(), timeout=0.5)
        for _ in range(100):
            if len(transports) == 2 and client.is_connected:
                break
            await asyncio.sleep(0.005)
        else:
            raise AssertionError("client did not reconnect after AUTH cancellation")

        assert len(disconnect_errors) == 1
        assert isinstance(disconnect_errors[0], MQTTError)
        assert str(disconnect_errors[0]) == "AUTH handler cancelled"
        assert not client._effect_pump.pending
    finally:
        await client.disconnect()


async def test_connect_cancellation_stops_blocked_auth_handler_promptly() -> None:
    transport = _AuthChallengeTransport()
    handler_entered = asyncio.Event()
    handler_release = asyncio.Event()

    async def blocking_auth(_packet: AuthPacket) -> None:
        handler_entered.set()
        await handler_release.wait()

    props = Properties({"authentication_method": "demo"})
    client = AsyncClient(
        client_id="auth-cancellation",
        protocol=MQTTProtocolVersion.MQTTv5,
        connect_properties=props,
        auth_handler=blocking_auth,
    )

    async def factory(
        host: str,
        port: int,
        *,
        ssl: object = None,
    ) -> _AuthChallengeTransport:
        return transport

    client._transport_factory = factory
    connecting = asyncio.create_task(client.connect("fake", timeout=30.0))
    await handler_entered.wait()
    connecting.cancel()
    done, _ = await asyncio.wait((connecting,), timeout=0.2)
    prompt = connecting in done
    if not prompt:
        # Keep the failing implementation from leaking its blocked AUTH task.
        handler_release.set()

    with pytest.raises(asyncio.CancelledError):
        await connecting

    assert prompt, "connect cancellation waited for the AUTH handler timeout"
    assert client.state is ConnectionState.DISCONNECTED
    assert client._transport is None
