"""Broker references are diagnostics, never implicit connection destinations."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient, Properties
from mqttium.codec.properties import encode_properties
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.errors import BrokerDisconnectError, MQTTError, ProtocolError
from mqttium.packets import encode_frame
from mqttium.protocol.reconnect import ReconnectPolicy
from tests.support import ScriptedBrokerTransport, wait_until

V5 = MQTTProtocolVersion.MQTTv5


def _disconnect(reason, props=None):
    return encode_frame(
        PacketType.DISCONNECT, 0, bytes((reason,)) + encode_properties(props, "DISCONNECT")
    )


@pytest.mark.parametrize("reason", (0x9C, 0x9D, 0x87))
@pytest.mark.parametrize("reference", (None, "new.example:1884"))
async def test_broker_failure_is_diagnostic_and_redirect_is_terminal(reason, reference):
    broker = ScriptedBrokerTransport(protocol=V5)
    destinations = []
    observed = []
    client = AsyncClient(
        "redirect",
        protocol=V5,
        reconnect=ReconnectPolicy(initial_delay=0, max_delay=0, stable_after=0),
    )

    async def factory(host, port, **kwargs):
        destinations.append((host, port))
        return broker

    client._transport_factory = factory
    client.on_disconnect = observed.append
    await client.connect("old.example", 1883)
    try:
        receipt = await client.publish("pending", b"value", qos=2)
        props = Properties({"server_reference": reference}) if reference else None
        broker.push_rx(_disconnect(reason, props))
        await wait_until(lambda: bool(observed))
        error = observed[0]
        assert isinstance(error, BrokerDisconnectError)
        assert error.reason_code == reason
        assert (error.properties.get("server_reference") if error.properties else None) == reference
        assert error.properties is client._last_disconnect.properties
        with pytest.raises(BrokerDisconnectError) as caught:
            await receipt.wait()
        assert caught.value is error
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(client.messages()), 1)
        assert destinations == [("old.example", 1883)]
        assert client._reconnect_task is None
    finally:
        await client.disconnect()


@pytest.mark.parametrize("reason", (0x9C, 0x9D))
@pytest.mark.parametrize("reference", (None, "new.example:1884"))
async def test_refused_connack_keeps_protocol_error_and_does_not_redirect(reason, reference):
    class RefusalBroker(ScriptedBrokerTransport):
        def handle_packet(self, raw):
            if raw.packet_type is PacketType.CONNECT:
                props = Properties({"server_reference": reference}) if reference else None
                self.push_rx(
                    encode_frame(
                        PacketType.CONNACK,
                        0,
                        bytes((0, reason)) + encode_properties(props, "CONNACK"),
                    )
                )
            else:
                super().handle_packet(raw)

    broker = RefusalBroker(protocol=V5)
    destinations = []
    client = AsyncClient("refusal", protocol=V5)

    async def factory(host, port, **kwargs):
        destinations.append((host, port))
        return broker

    client._transport_factory = factory
    try:
        with pytest.raises(ProtocolError, match="Connection refused"):
            await client.connect("old.example", 1883)
        assert isinstance(client._disconnect_exc, ProtocolError)
        assert destinations == [("old.example", 1883)]
        assert client._reconnect_task is None
    finally:
        await client.disconnect()


@pytest.mark.parametrize("kind", ("zero", "eof", "explicit", "malformed", "prior", "local"))
async def test_specific_failure_and_normal_disconnect_semantics_are_preserved(kind):
    broker = ScriptedBrokerTransport(protocol=V5)
    client = AsyncClient("cause-priority", protocol=V5, reconnect=ReconnectPolicy(enabled=False))
    observed = []
    client.on_disconnect = observed.append

    async def factory(*args, **kwargs):
        return broker

    client._transport_factory = factory
    await client.connect("fake")
    prior = OSError("earlier failure")
    try:
        if kind == "explicit":
            await client.disconnect()
        elif kind == "eof":
            broker.push_rx(b"")
        else:
            if kind == "prior":
                client._disconnect_exc = prior
            elif kind == "local":
                client._local_terminal_failure = prior
            props = Properties({"session_expiry_interval": 60}) if kind == "malformed" else None
            # Increasing expiry from the default zero is forbidden. The engine
            # emits DISCONNECTED before diagnosing this property conflict.
            broker.push_rx(_disconnect(0 if kind == "zero" else 0x9C, props))
        await wait_until(lambda: bool(observed))
        error = observed[0]
        if kind in ("prior", "local"):
            assert error is prior
        elif kind == "malformed":
            assert isinstance(error, ProtocolError)
        elif kind == "explicit":
            assert error is None
        else:
            assert type(error) is MQTTError
        assert not isinstance(error, BrokerDisconnectError)
    finally:
        await client.disconnect()


def test_removed_follow_option_is_not_silently_accepted():
    with pytest.raises(TypeError, match="follow_server_reference"):
        ReconnectPolicy(follow_server_reference=True)
