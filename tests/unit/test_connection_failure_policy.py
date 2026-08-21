"""Connection refusal, protocol failure, and terminal-disconnect policy."""

from __future__ import annotations

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.properties import CONNACK, SUBACK, encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.errors import ProtocolError
from mqttium.packets import encode_disconnect, encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.protocol.negotiated import NegotiatedSettings
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.types import Properties
from tests.support import QueueTransport, feed_engine


def _connect_v5(engine: ProtocolEngine) -> None:
    engine.begin_connect()
    feed_engine(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    engine.take_effects()
    assert engine.state is ConnectionState.CONNECTED


def test_local_protocol_disconnect_is_terminal_when_packet_cannot_fit() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="rc2", protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    engine.negotiated = NegotiatedSettings(maximum_packet_size=2)

    engine._protocol_disconnect(0x82)
    effects = engine.take_effects()

    assert engine.state is ConnectionState.DISCONNECTED
    assert not [effect for effect in effects if effect.kind is EffectKind.SEND]
    disconnected = [effect.data for effect in effects if effect.kind is EffectKind.DISCONNECTED]
    assert len(disconnected) == 1
    assert disconnected[0].reason_code == 0x82


@pytest.mark.parametrize(
    "reason_code",
    [
        0x87,
        0x89,
        0x8B,
        0x8C,
        0x8D,
        0x8E,
        0x8F,
        0x9A,
        0x9B,
        0x9C,
        0x9D,
        0x9E,
        0x9F,
        0xA0,
        0xA1,
        0xA2,
    ],
)
def test_client_disconnect_rejects_non_client_reason_codes(reason_code: int) -> None:
    with pytest.raises(ProtocolError):
        encode_disconnect(reason_code, MQTTProtocolVersion.MQTTv5)


def test_client_disconnect_rejects_server_reference() -> None:
    properties = Properties(values={"server_reference": "other.example"})

    with pytest.raises(ProtocolError, match="Server-only"):
        encode_disconnect(0x00, MQTTProtocolVersion.MQTTv5, properties)

    assert encode_disconnect(0x90, MQTTProtocolVersion.MQTTv5)


def test_connect_authentication_data_requires_method_without_mutating_state() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="rc2-auth",
            protocol=MQTTProtocolVersion.MQTTv5,
            connect_properties=Properties(values={"authentication_data": b"opaque"}),
        )
    )

    with pytest.raises(ProtocolError, match="authentication_data requires"):
        engine.begin_connect()

    assert engine.state is ConnectionState.NEW


def test_connack_auth_mismatch_finishes_engine_teardown() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="rc2-auth",
            protocol=MQTTProtocolVersion.MQTTv5,
            connect_properties=Properties(values={"authentication_method": "SCRAM"}),
        )
    )
    engine.begin_connect()
    properties = Properties(values={"authentication_method": "OTHER"})
    remaining = b"\x00\x00" + encode_properties(properties, CONNACK)

    feed_engine(engine, encode_frame(PacketType.CONNACK, 0, remaining))
    effects = engine.take_effects()

    assert engine.state is ConnectionState.DISCONNECTED
    assert any(effect.kind is EffectKind.DISCONNECTED for effect in effects)
    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)


def test_request_problem_information_zero_rejects_suback_without_settling() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="rc2-rpi",
            protocol=MQTTProtocolVersion.MQTTv5,
            connect_properties=Properties(values={"request_problem_information": 0}),
        )
    )
    _connect_v5(engine)
    mid = engine.queue_subscribe("rc2/topic")
    engine.take_effects()
    properties = Properties(values={"reason_string": "diagnostic"})
    remaining = mid.to_bytes(2, "big") + encode_properties(properties, SUBACK) + b"\x00"

    feed_engine(engine, encode_frame(PacketType.SUBACK, 0, remaining))
    effects = engine.take_effects()

    assert engine.state is ConnectionState.DISCONNECTED
    assert mid not in engine._pending_sub_mids
    assert not engine.outbound.packet_ids.in_use(mid)
    assert not any(effect.kind is EffectKind.SUBACK for effect in effects)
    assert any(effect.kind is EffectKind.DISCONNECTED for effect in effects)
    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)


@pytest.mark.parametrize(
    "reason_code",
    [
        0x81,
        0x82,
        0x84,
        0x85,
        0x86,
        0x87,
        0x8A,
        0x8C,
        0x90,
        0x95,
        0x99,
        0x9A,
        0x9B,
        0x9C,
        0x9D,
    ],
)
def test_reconnect_stops_on_permanent_v5_connack_refusals(reason_code: int) -> None:
    policy = ReconnectPolicy(enabled=True)
    assert not policy.should_retry(reason_code, MQTTProtocolVersion.MQTTv5)


@pytest.mark.parametrize("reason_code", [0x80, 0x83, 0x88, 0x89, 0x97, 0x9F])
def test_reconnect_keeps_transient_v5_failures_retryable(reason_code: int) -> None:
    policy = ReconnectPolicy(enabled=True)
    assert policy.should_retry(reason_code, MQTTProtocolVersion.MQTTv5)


class _OrderedCloseTransport(QueueTransport):
    def __init__(self) -> None:
        super().__init__()
        self.events: list[tuple[str, bytes]] = []

    async def write(self, data: bytes) -> None:
        if self.is_closing():
            raise AssertionError("write attempted after transport close")
        self.events.append(("write", bytes(data)))

    async def write_many(self, parts: list[bytes]) -> None:
        for part in parts:
            await self.write(part)

    async def close(self) -> None:
        self.events.append(("close", b""))
        await super().close()


async def test_local_protocol_disconnect_writes_before_transport_close() -> None:
    transport = _OrderedCloseTransport()
    client = AsyncClient(protocol=MQTTProtocolVersion.MQTTv5)
    client._transport = transport
    client._write_pump.start(transport)
    client._engine.state = ConnectionState.CONNECTED

    try:
        client._engine._protocol_disconnect(0x82)
        client._collect_effects_locked()
        await client._drain_effects()

        assert transport.events == [
            ("write", encode_disconnect(0x82, MQTTProtocolVersion.MQTTv5)),
            ("close", b""),
        ]
    finally:
        await client._write_pump.stop()


class _InvalidConnackTransport(QueueTransport):
    def __init__(self) -> None:
        super().__init__()
        self.sent_connack = False

    async def write(self, data: bytes) -> None:
        del data
        if not self.sent_connack:
            self.sent_connack = True
            self.push_rx(b"\x20\x03\x00\x00\x00")


async def test_connect_surfaces_local_connack_protocol_failure_and_closes_transport() -> None:
    transport = _InvalidConnackTransport()
    client = AsyncClient(protocol=MQTTProtocolVersion.MQTTv5)

    async def factory(*args: object, **kwargs: object) -> _InvalidConnackTransport:
        del args, kwargs
        return transport

    client._transport_factory = factory  # type: ignore[assignment]

    with pytest.raises(ProtocolError, match="Assigned Client Identifier"):
        await client.connect("broker", 1883, timeout=1.0)

    assert transport.is_closing()
