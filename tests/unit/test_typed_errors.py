"""Failures a service must tell apart arrive as typed MQTTium errors."""

from __future__ import annotations

import asyncio
import socket

import pytest

from mqttium import (
    ConnectError,
    MQTTError,
    ProtocolError,
    PublishRejectedError,
    QoS,
    SubscribeError,
)
from mqttium.api import AsyncClient, SubscribeResult
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.packets import encode_frame
from tests.support import ScriptedBrokerTransport, transport_factory

V5 = MQTTProtocolVersion.MQTTv5


async def test_unreachable_broker_raises_connect_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # Refuse deterministically: a closed port is refused at once on Linux but
    # only times out on Windows, where the connection attempt is retried.
    async def refused(*args: object, **kwargs: object) -> None:
        raise ConnectionRefusedError(111, "Connection refused")

    monkeypatch.setattr(asyncio.get_running_loop(), "create_connection", refused)
    client = AsyncClient("unreachable")
    with pytest.raises(ConnectError) as failed:
        await client.connect("127.0.0.1", 1883, timeout=2)
    assert isinstance(failed.value, MQTTError)
    assert isinstance(failed.value, OSError)
    assert isinstance(failed.value.__cause__, ConnectionRefusedError)
    await client.disconnect()


async def test_unknown_host_raises_connect_error(monkeypatch: pytest.MonkeyPatch) -> None:
    async def unresolvable(*args: object, **kwargs: object) -> None:
        raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", unresolvable)
    client = AsyncClient("dns")
    with pytest.raises(ConnectError) as failed:
        await client.connect("broker.example", 1883, timeout=5)
    assert isinstance(failed.value.__cause__, socket.gaierror)
    await client.disconnect()


class _RejectingBroker(ScriptedBrokerTransport):
    """Answer every QoS 1 PUBLISH with PUBACK 0x87 and a Reason String."""

    def handle_packet(self, raw) -> None:  # noqa: ANN001
        if raw.packet_type is PacketType.PUBLISH:
            mid = self._publish_mid(raw)
            reason_string = b"no rights"
            properties = bytes((0x1F, 0, len(reason_string))) + reason_string
            body = mid.to_bytes(2, "big") + b"\x87" + bytes((len(properties),)) + properties
            self.push_rx(encode_frame(PacketType.PUBACK, 0, body))
        else:
            super().handle_packet(raw)

    @staticmethod
    def _publish_mid(raw) -> int:  # noqa: ANN001
        topic_length = int.from_bytes(raw.remaining[:2], "big")
        start = 2 + topic_length
        return int.from_bytes(raw.remaining[start : start + 2], "big")


async def test_refused_publication_carries_reason_code_and_properties() -> None:
    broker = _RejectingBroker(protocol=V5)
    client = AsyncClient("rejected", protocol=V5)
    client._transport_factory = transport_factory(broker)
    try:
        await client.connect("broker")
        receipt = await client.publish("t/denied", b"x", qos=1)
        with pytest.raises(PublishRejectedError) as rejected:
            await asyncio.wait_for(receipt.wait(), 2)
        assert rejected.value.reason_code == 0x87
        assert rejected.value.properties is not None
        assert rejected.value.properties.get("reason_string") == "no rights"
        assert isinstance(rejected.value, ProtocolError)
    finally:
        await client.disconnect()


async def test_refused_filter_raises_subscribe_error_with_the_full_result() -> None:
    broker = ScriptedBrokerTransport(suback_reason=0x80)
    client = AsyncClient("acl")
    client._transport_factory = transport_factory(broker)
    try:
        await client.connect("broker")
        with pytest.raises(SubscribeError) as refused:
            await client.subscribe("forbidden/#", qos=1)
        result = refused.value.result
        assert result.reason_codes == (0x80,)
        assert result.granted_qos == (None,)
    finally:
        await client.disconnect()


def test_granted_qos_follows_each_reason_code() -> None:
    result = SubscribeResult(mid=1, reason_codes=(0, 1, 2, 0x87))
    assert result.granted_qos == (QoS.AT_MOST_ONCE, QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE, None)


@pytest.mark.parametrize("level", [4, 5])
def test_protocol_accepts_its_wire_level(level: int) -> None:
    client = AsyncClient("level", protocol=level)
    assert client._engine.config.protocol is MQTTProtocolVersion(level)


@pytest.mark.parametrize(("value", "error"), [(6, ValueError), ("5", TypeError), (True, TypeError)])
def test_protocol_refuses_other_values(value: object, error: type[Exception]) -> None:
    with pytest.raises(error, match="protocol|MQTT"):
        AsyncClient("level", protocol=value)  # type: ignore[arg-type]
