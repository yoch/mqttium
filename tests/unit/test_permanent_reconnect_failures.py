"""Permanent peer/security failures stop retries without breaking transient recovery."""

import asyncio
import ssl

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MalformedPacketError, ProtocolError
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.reconnect import ReconnectPolicy
from tests.support import ScriptedBrokerTransport, wait_until


class QuietBroker(ScriptedBrokerTransport):
    """Accept CONNECT but retain outbound QoS work without acknowledging it."""

    def __init__(self, *, reason=0, protocol=MQTTProtocolVersion.MQTTv5, connack=None):
        super().__init__(protocol=protocol)
        self.reason = reason
        self.connack = connack
        self.published = asyncio.Event()

    def handle_packet(self, raw):
        if raw.packet_type is PacketType.CONNECT:
            body = bytes((0, self.reason))
            if self.protocol is MQTTProtocolVersion.MQTTv5:
                body += b"\x00"
            packet = encode_frame(PacketType.CONNACK, 0, body)
            self.push_rx(packet if self.connack is None else self.connack)
        elif raw.packet_type is PacketType.PUBLISH:
            self.published.set()
        else:
            super().handle_packet(raw)


def retry_policy(*, stable_after=0, max_retries=None):
    return ReconnectPolicy(
        initial_delay=0.002,
        multiplier=2,
        max_delay=0.008,
        stable_after=stable_after,
        max_retries=max_retries,
    )


async def stopped(client):
    await wait_until(lambda: client._delivery.closed.is_set())
    await wait_until(lambda: not any(client._running_tasks().values()))
    assert client._transport is None
    assert not client.is_connected


@pytest.mark.parametrize(
    ("wire", "error"),
    [
        (b"\xe1\x00", MalformedPacketError),
        # A zero topic alias has legal framing but violates MQTT 5.
        (b"\x30\x07\x00\x01t\x03\x23\x00\x00", MalformedPacketError),
        # A second CONNACK on an established connection is a protocol error.
        (b"\x20\x03\x00\x00\x00", ProtocolError),
    ],
)
async def test_malformed_session_closes_stream_and_settles_pending(wire, error):
    transport = QuietBroker()
    client = AsyncClient(
        "terminal-peer",
        protocol=MQTTProtocolVersion.MQTTv5,
        keepalive=0,
        reconnect=retry_policy(),
    )
    attempts = []
    disconnected = []

    async def factory(*args, **kwargs):
        attempts.append(True)
        return transport

    client._transport_factory = factory
    client.on_disconnect = disconnected.append
    try:
        await client.connect("unused")
        stream = client.messages()
        receipt = await client.publish("pending", b"x", qos=1)
        await asyncio.wait_for(transport.published.wait(), 1)
        transport.push_rx(wire)
        await stopped(client)
        assert len(attempts) == 1
        assert len(disconnected) == 1
        assert isinstance(disconnected[0], error)
        with pytest.raises(error) as caught:
            await receipt.wait()
        assert caught.value is disconnected[0]
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(stream), 1)
        assert client.stats().receipts.publish == 0
    finally:
        await client.disconnect()


async def test_certificate_failure_reports_cause_and_allows_explicit_repair():
    first = QuietBroker()
    replacement = QuietBroker()
    cause = ssl.SSLCertVerificationError(1, "certificate verify failed: test CA")
    client = AsyncClient(
        "terminal-tls",
        protocol=MQTTProtocolVersion.MQTTv5,
        keepalive=0,
        reconnect=retry_policy(),
    )
    attempts = []
    disconnected = []

    async def factory(*args, **kwargs):
        attempts.append(True)
        if len(attempts) == 1:
            return first
        raise cause

    client._transport_factory = factory
    client.on_disconnect = disconnected.append
    try:
        await client.connect("unused")
        stream = client.messages()
        receipt = await client.publish("pending", b"x", qos=1)
        await asyncio.wait_for(first.published.wait(), 1)
        first.push_rx(b"")
        await stopped(client)
        assert len(attempts) == 2
        assert disconnected[-1] is cause
        assert sum(item is cause for item in disconnected) == 1
        with pytest.raises(ssl.SSLCertVerificationError) as caught:
            await receipt.wait()
        assert caught.value is cause
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(stream), 1)
        assert client._local_terminal_failure is None

        async def repaired(*args, **kwargs):
            return replacement

        client._transport_factory = repaired
        await client.connect("repaired")
        assert client.is_connected
        with pytest.raises(StopAsyncIteration):
            await anext(stream)
        new_stream = client.messages()
        packet = PublishPacket(
            topic="repaired", payload=b"ok", qos=QoS.AT_MOST_ONCE, retain=False, dup=False
        )
        replacement.push_rx(packet.encode(MQTTProtocolVersion.MQTTv5))
        message = await asyncio.wait_for(anext(new_stream), 1)
        assert message.payload == b"ok"
        await new_stream.aclose()
    finally:
        await client.disconnect()


@pytest.mark.parametrize("phase", ["connack", "stability"])
async def test_protocol_failure_during_reconnect_never_starts_another_attempt(phase):
    first = QuietBroker()
    second = QuietBroker(connack=b"\xe1\x00" if phase == "connack" else None)
    client = AsyncClient(
        "terminal-retry",
        protocol=MQTTProtocolVersion.MQTTv5,
        keepalive=0,
        reconnect=retry_policy(stable_after=0.02),
    )
    attempts = []

    async def factory(*args, **kwargs):
        attempts.append(True)
        return first if len(attempts) == 1 else second

    client._transport_factory = factory
    try:
        await client.connect("unused")
        first.push_rx(b"")
        if phase == "stability":
            await wait_until(lambda: len(attempts) == 2 and client.is_connected)
            second.push_rx(b"\xe1\x00")
        await stopped(client)
        assert len(attempts) == 2
        assert isinstance(client._disconnect_exc, MalformedPacketError)
    finally:
        await client.disconnect()


@pytest.mark.parametrize(
    "failure",
    [
        ConnectionResetError("reset"),
        ConnectionRefusedError("refused"),
        TimeoutError("connect timeout"),
        ssl.SSLEOFError(8, "unexpected EOF"),
    ],
)
async def test_transient_setup_failure_keeps_backoff_and_same_application_stream(
    failure, monkeypatch
):
    monkeypatch.setattr("mqttium.protocol.reconnect.random.uniform", lambda *_: 1.0)
    first = QuietBroker()
    second = QuietBroker()
    client = AsyncClient(
        "transient-retry",
        protocol=MQTTProtocolVersion.MQTTv5,
        keepalive=0,
        reconnect=retry_policy(),
    )
    attempts = []
    observed_delays = []
    next_delay = client._reconnect.next_delay

    def measured_delay():
        delay = next_delay()
        observed_delays.append(delay)
        return delay

    client._reconnect.next_delay = measured_delay

    async def factory(*args, **kwargs):
        attempts.append(True)
        if len(attempts) == 2:
            raise failure
        return first if len(attempts) == 1 else second

    client._transport_factory = factory
    try:
        await client.connect("unused")
        stream = client.messages()
        first.push_rx(b"")
        await wait_until(lambda: len(attempts) == 3 and client.is_connected)
        assert observed_delays == [0.002, 0.004]
        packet = PublishPacket(
            topic="after/retry", payload=b"ok", qos=QoS.AT_MOST_ONCE, retain=False, dup=False
        )
        second.push_rx(packet.encode(MQTTProtocolVersion.MQTTv5))
        assert (await asyncio.wait_for(anext(stream), 1)).payload == b"ok"
        await stream.aclose()
    finally:
        await client.disconnect()
        await wait_until(lambda: not any(client._running_tasks().values()))


@pytest.mark.parametrize(
    ("protocol", "reason", "retry"),
    [
        (MQTTProtocolVersion.MQTTv311, 3, True),
        (MQTTProtocolVersion.MQTTv311, 5, False),
        (MQTTProtocolVersion.MQTTv5, 0x88, True),
        (MQTTProtocolVersion.MQTTv5, 0x89, True),
        (MQTTProtocolVersion.MQTTv5, 0x87, False),
    ],
)
async def test_connack_refusals_keep_reason_code_policy(protocol, reason, retry):
    brokers = [
        QuietBroker(protocol=protocol),
        QuietBroker(protocol=protocol, reason=reason),
        QuietBroker(protocol=protocol),
    ]
    client = AsyncClient("reason-policy", protocol=protocol, keepalive=0, reconnect=retry_policy())
    attempts = []

    async def factory(*args, **kwargs):
        attempts.append(True)
        return brokers[min(len(attempts) - 1, 2)]

    client._transport_factory = factory
    try:
        await client.connect("unused")
        stream = client.messages()
        brokers[0].push_rx(b"")
        if retry:
            await wait_until(lambda: len(attempts) == 3 and client.is_connected)
            assert not client._delivery.closed.is_set()
        else:
            await stopped(client)
            assert len(attempts) == 2
            with pytest.raises(StopAsyncIteration):
                await asyncio.wait_for(anext(stream), 1)
        await stream.aclose()
    finally:
        await client.disconnect()
        await wait_until(lambda: not any(client._running_tasks().values()))
