"""AUTH packet + enhanced authentication engine/client tests."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.packets import AuthPacket, encode_frame
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine
from mqttium.types import Properties


def test_auth_packet_roundtrip() -> None:
    props = Properties()
    props.set("authentication_method", "SCRAM-SHA-1")
    props.set("authentication_data", b"\x01\x02\x03")
    wire = AuthPacket(reason_code=0x18, properties=props).encode()
    assert wire[0] == PacketType.AUTH
    dec = IncrementalDecoder()
    dec.feed(wire)
    raw = dec.next_packet()
    assert raw is not None
    parsed = AuthPacket.decode(raw.remaining)
    assert parsed.reason_code == 0x18
    assert parsed.properties is not None
    assert parsed.properties.get("authentication_method") == "SCRAM-SHA-1"
    assert parsed.properties.get("authentication_data") == b"\x01\x02\x03"


def test_engine_rejects_auth_without_accept() -> None:
    engine = ProtocolEngine(
        EngineConfig(client_id="c", protocol=MQTTProtocolVersion.MQTTv5, accept_auth=False)
    )
    engine.state = ConnectionState.CONNECTED
    raw = RawPacket(
        packet_type=PacketType.AUTH,
        flags=0,
        remaining=AuthPacket(reason_code=0x18).encode()[2:],  # wrong — need proper remaining
    )
    # Build remaining correctly via encode then strip fixed header.
    full = AuthPacket(reason_code=0x18).encode()
    # fixed header: 1 type + 1 VBI (small)
    from mqttium.codec.vbi import decode_vbi

    _, pos = decode_vbi(full, 1)
    raw = RawPacket(packet_type=PacketType.AUTH, flags=0, remaining=full[pos:])
    engine.handle_raw(raw)
    effects = engine.take_effects()
    kinds = [e.kind for e in effects]
    assert EffectKind.SEND in kinds
    assert EffectKind.DISCONNECTED in kinds
    assert engine.state == ConnectionState.DISCONNECTED


def test_engine_emits_auth_when_accepted() -> None:
    connect_props = Properties()
    connect_props.set("authentication_method", "demo")
    engine = ProtocolEngine(
        EngineConfig(
            client_id="c",
            protocol=MQTTProtocolVersion.MQTTv5,
            accept_auth=True,
            connect_properties=connect_props,
        )
    )
    engine.begin_connect()
    engine.take_effects()
    auth_props = Properties()
    auth_props.set("authentication_method", "demo")
    full = AuthPacket(reason_code=0x18, properties=auth_props).encode()
    from mqttium.codec.vbi import decode_vbi

    _, pos = decode_vbi(full, 1)
    engine.handle_raw(RawPacket(PacketType.AUTH, 0, full[pos:]))
    effects = engine.take_effects()
    assert any(e.kind is EffectKind.AUTH for e in effects)
    auth = next(e.data for e in effects if e.kind is EffectKind.AUTH)
    assert auth.reason_code == 0x18


@pytest.mark.asyncio
async def test_async_client_auth_handler_exchange() -> None:
    class FakeTransport:
        def __init__(self) -> None:
            self._rx: asyncio.Queue[bytes] = asyncio.Queue()
            self._decoder = IncrementalDecoder()
            self._closing = False
            self.written: list[bytes] = []

        async def write(self, data: bytes) -> None:
            self.written.append(data)
            self._decoder.feed(data)
            for raw in self._decoder.drain_packets():
                if raw.packet_type is PacketType.CONNECT:
                    # Challenge then CONNACK success.
                    props = Properties()
                    props.set("authentication_method", "demo")
                    challenge = AuthPacket(reason_code=0x18, properties=props)
                    self._rx.put_nowait(challenge.encode())
                elif raw.packet_type is PacketType.AUTH:
                    # Successful enhanced-auth CONNACK repeats the method.
                    from mqttium.codec.properties import encode_properties

                    props = Properties()
                    props.set("authentication_method", "demo")
                    body = bytearray([0x00, 0x00])
                    body.extend(encode_properties(props, "CONNACK"))
                    self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, body))

        async def read(self, n: int = 65536) -> bytes:
            return await self._rx.get()

        async def close(self) -> None:
            self._closing = True
            self._rx.put_nowait(b"")

        def is_closing(self) -> bool:
            return self._closing

    def handler(challenge: AuthPacket) -> AuthPacket:
        assert challenge.reason_code == 0x18
        props = Properties()
        props.set("authentication_method", "demo")
        props.set("authentication_data", b"token")
        # [MQTT-4.12.0-3]: a Client answering a Server AUTH must use 0x18. Table
        # 3-11 reserves 0x00 (Success) for the Server, which concludes the
        # exchange with CONNACK rather than with an AUTH of its own.
        return AuthPacket(reason_code=0x18, properties=props)

    connect_props = Properties()
    connect_props.set("authentication_method", "demo")
    client = AsyncClient(
        client_id="auth-c",
        protocol=MQTTProtocolVersion.MQTTv5,
        connect_properties=connect_props,
        auth_handler=handler,
    )
    fake = FakeTransport()

    async def factory(host: str, port: int, *, ssl: object = None) -> FakeTransport:
        return fake

    client._transport_factory = factory  # type: ignore[assignment]
    connack = await client.connect("fake", 1883)
    assert connack.reason_code == 0
    assert any(w[0] == PacketType.AUTH for w in fake.written)
    await client.disconnect()
