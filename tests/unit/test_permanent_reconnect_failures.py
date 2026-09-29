"""Permanent peer/security failures stop retries without breaking transient recovery."""

import asyncio
import ssl

import pytest

from mqttium.api import AsyncClient, Properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import ConnectRefusedError, MalformedPacketError, ProtocolError
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
    states = []

    async def factory(*args, **kwargs):
        attempts.append(True)
        return transport

    def on_disconnect(cause):
        disconnected.append(cause)
        states.append(client.state)

    client._transport_factory = factory
    client.on_disconnect = on_disconnect
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
        # The hook already sees the final state.
        assert states == [ConnectionState.DISCONNECTED]
        with pytest.raises(error) as caught:
            await receipt.wait()
        assert caught.value is disconnected[0]
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(stream), 1)
        assert client.stats().receipts.publish == 0
    finally:
        await client.disconnect()


@pytest.mark.parametrize(
    ("wire", "error"),
    [
        # A second CONNACK on an established connection is a protocol error.
        (b"\x20\x03\x00\x00\x00", ProtocolError),
        # A local decode budget ends this connection, not the client.
        pytest.param(
            PublishPacket(
                topic="amplified",
                payload=b"",
                qos=QoS.AT_MOST_ONCE,
                retain=False,
                dup=False,
                properties=Properties({"user_property": [("", "")] * 1025}),
            ).encode(MQTTProtocolVersion.MQTTv5),
            ProtocolError,
            id="repeatable-property-budget",
        ),
    ],
)
async def test_peer_protocol_error_ends_the_connection_and_is_retried(wire, error):
    first = QuietBroker()
    second = QuietBroker()
    client = AsyncClient(
        "retried-peer",
        protocol=MQTTProtocolVersion.MQTTv5,
        keepalive=0,
        reconnect=retry_policy(),
    )
    attempts = []
    disconnected = []
    states = []

    async def factory(*args, **kwargs):
        attempts.append(True)
        return first if len(attempts) == 1 else second

    def on_disconnect(cause):
        disconnected.append(cause)
        states.append(client.state)

    client._transport_factory = factory
    client.on_disconnect = on_disconnect
    try:
        await client.connect("unused")
        stream = client.messages()
        first.push_rx(wire)
        await wait_until(lambda: len(attempts) == 2 and client.is_connected)
        assert isinstance(disconnected[0], error)
        assert states == [ConnectionState.RECONNECTING]
        packet = PublishPacket(
            topic="after/retry", payload=b"ok", qos=QoS.AT_MOST_ONCE, retain=False, dup=False
        )
        second.push_rx(packet.encode(MQTTProtocolVersion.MQTTv5))
        assert (await asyncio.wait_for(anext(stream), 1)).payload == b"ok"
        await stream.aclose()
    finally:
        await client.disconnect()


async def test_certificate_failure_is_retried_until_the_rotation_completes():
    first = QuietBroker()
    rotated = QuietBroker()
    cause = ssl.SSLCertVerificationError(1, "certificate verify failed: test CA")
    client = AsyncClient(
        "retried-tls",
        protocol=MQTTProtocolVersion.MQTTv5,
        keepalive=0,
        reconnect=retry_policy(),
    )
    attempts = []
    states = []

    async def factory(*args, **kwargs):
        attempts.append(True)
        if len(attempts) == 1:
            return first
        if len(attempts) == 2:
            states.append(client.state)
            raise cause
        return rotated

    client._transport_factory = factory
    try:
        await client.connect("unused")
        stream = client.messages()
        first.push_rx(b"")
        # A rejected certificate during a rotation is transient: the policy
        # keeps trying and the same application stream resumes.
        await wait_until(lambda: len(attempts) == 3 and client.is_connected)
        assert states == [ConnectionState.RECONNECTING]
        assert client._local_terminal_failure is None
        packet = PublishPacket(
            topic="rotated", payload=b"ok", qos=QoS.AT_MOST_ONCE, retain=False, dup=False
        )
        rotated.push_rx(packet.encode(MQTTProtocolVersion.MQTTv5))
        assert (await asyncio.wait_for(anext(stream), 1)).payload == b"ok"
        await stream.aclose()
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
    ("protocol", "reason", "retry_refused", "retry"),
    [
        (MQTTProtocolVersion.MQTTv311, 3, False, True),
        (MQTTProtocolVersion.MQTTv311, 5, False, False),
        (MQTTProtocolVersion.MQTTv311, 5, True, True),
        (MQTTProtocolVersion.MQTTv5, 0x88, False, True),
        (MQTTProtocolVersion.MQTTv5, 0x89, False, True),
        (MQTTProtocolVersion.MQTTv5, 0x87, False, False),
        (MQTTProtocolVersion.MQTTv5, 0x87, True, True),
    ],
)
async def test_connack_refusals_keep_reason_code_policy(protocol, reason, retry_refused, retry):
    brokers = [
        QuietBroker(protocol=protocol),
        QuietBroker(protocol=protocol, reason=reason),
        QuietBroker(protocol=protocol),
    ]
    policy = ReconnectPolicy(
        initial_delay=0.002, max_delay=0.008, stable_after=0, retry_refused=retry_refused
    )
    client = AsyncClient("reason-policy", protocol=protocol, keepalive=0, reconnect=policy)
    attempts = []
    disconnected = []

    async def factory(*args, **kwargs):
        attempts.append(True)
        return brokers[min(len(attempts) - 1, 2)]

    def on_disconnect(cause):
        disconnected.append((cause, client.state))

    client._transport_factory = factory
    client.on_disconnect = on_disconnect
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
            # The refusal is typed, carries its code, and the hook that
            # reports it already sees the final state.
            refusal, state = disconnected[-1]
            assert isinstance(refusal, ConnectRefusedError)
            assert refusal.reason_code == reason
            assert state is ConnectionState.DISCONNECTED
            assert client.stats().state is ConnectionState.DISCONNECTED
        await stream.aclose()
    finally:
        await client.disconnect()
        await wait_until(lambda: not any(client._running_tasks().values()))


async def test_session_taken_over_stops_instead_of_evicting_forever():
    first = QuietBroker()
    client = AsyncClient(
        "taken-over",
        protocol=MQTTProtocolVersion.MQTTv5,
        keepalive=0,
        reconnect=retry_policy(),
    )
    attempts = []
    states = []

    async def factory(*args, **kwargs):
        attempts.append(True)
        return first

    client._transport_factory = factory
    client.on_disconnect = lambda cause: states.append(client.state)
    try:
        await client.connect("unused")
        first.push_rx(b"\xe0\x02\x8e\x00")  # DISCONNECT: Session taken over
        await stopped(client)
        assert len(attempts) == 1
        assert states == [ConnectionState.DISCONNECTED]
    finally:
        await client.disconnect()


async def test_stats_report_reconnecting_during_backoff():
    first = QuietBroker()
    client = AsyncClient(
        "reconnecting-stats",
        protocol=MQTTProtocolVersion.MQTTv5,
        keepalive=0,
        reconnect=ReconnectPolicy(initial_delay=30.0, max_delay=30.0),
    )

    async def factory(*args, **kwargs):
        return first

    client._transport_factory = factory
    try:
        await client.connect("unused")
        first.push_rx(b"")
        await wait_until(lambda: client.state is ConnectionState.RECONNECTING)
        assert client.stats().state is ConnectionState.RECONNECTING
        assert not client.is_connected
    finally:
        await client.disconnect()
    assert client.state is ConnectionState.DISCONNECTED
