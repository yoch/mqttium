"""auth_handler runs in its own task, outside the effect lane and the reader.

The handler is user code: it may await the client (publish, subscribe,
disconnect), a message delivered by the same read, or an existing receipt.
None of these may deadlock. Its answer is sent only while the exchange still
waits for it.
"""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import BrokerDisconnectError, ProtocolError
from mqttium.packets import AuthPacket, PubAckPacket, PublishPacket, encode_frame
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Properties
from tests.support import QueueTransport, transport_factory, wait_until

V5 = MQTTProtocolVersion.MQTTv5
METHOD = Properties({"authentication_method": "demo"})


def _auth(reason: int) -> bytes:
    return AuthPacket(reason_code=reason, properties=METHOD).encode(V5)


class _ReauthBroker(QueueTransport):
    """Accepts enhanced auth, acknowledges QoS 1 on request, records AUTH."""

    def __init__(self, *, auto_puback: bool = True) -> None:
        super().__init__()
        self.decoder = IncrementalDecoder()
        self.auto_puback = auto_puback
        self.client_auth: list[int] = []
        self.published: list[int] = []

    async def write(self, data: bytes | tuple[bytes, bytes]) -> None:
        wire = data if isinstance(data, bytes) else data[0] + data[1]
        self.decoder.feed(wire)
        for raw in self.decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                body = b"\x00\x00" + encode_properties(METHOD, "CONNACK")
                self.push_rx(encode_frame(PacketType.CONNACK, 0, body))
            elif raw.packet_type is PacketType.AUTH:
                self.client_auth.append(raw.remaining[0])
            elif raw.packet_type is PacketType.PUBLISH:
                publish = PublishPacket.decode(raw.flags, raw.remaining, V5)
                if publish.mid is not None:
                    self.published.append(publish.mid)
                    if self.auto_puback:
                        self.push_rx(PubAckPacket(mid=publish.mid).encode(V5))

    async def write_many(self, parts: list[bytes]) -> None:
        await self.write(b"".join(parts))


@pytest.fixture(params=["normal", "eager"])
async def task_factory(request):
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    if request.param == "eager" and not hasattr(asyncio, "eager_task_factory"):
        pytest.skip("eager task factory requires Python 3.12+")
    if request.param == "eager":
        loop.set_task_factory(asyncio.eager_task_factory)
    try:
        yield
    finally:
        loop.set_task_factory(previous)


async def _client(handler, broker: _ReauthBroker, **kwargs) -> AsyncClient:
    client = AsyncClient(
        "auth-ownership",
        protocol=V5,
        connect_properties=METHOD,
        auth_handler=handler,
        auth_timeout=2.0,
        keepalive=0,
        **kwargs,
    )
    client._transport_factory = transport_factory(broker)
    await client.connect("fake")
    await client.auth()  # Re-authenticate (0x19)
    return client


async def test_handler_can_publish_and_answer_the_challenge() -> None:
    # #502: a client operation from the handler used to wait for the pump that
    # was running the handler.
    broker = _ReauthBroker()
    done = asyncio.Event()
    client: AsyncClient | None = None

    async def handler(packet: AuthPacket) -> AuthPacket:
        assert client is not None
        receipt = await client.publish("from/handler", b"x", qos=QoS.AT_LEAST_ONCE)
        await receipt.wait()
        done.set()
        return AuthPacket(reason_code=0x18, properties=METHOD)

    client = await _client(handler, broker)
    try:
        broker.push_rx(_auth(0x18))
        await asyncio.wait_for(done.wait(), 1)
        await wait_until(lambda: broker.client_auth == [0x19, 0x18])
    finally:
        await client.disconnect()


async def test_handler_can_disconnect() -> None:
    # #501: disconnect() from the handler used to deadlock pump and reader.
    broker = _ReauthBroker()
    client: AsyncClient | None = None
    returned = asyncio.Event()

    async def handler(packet: AuthPacket) -> None:
        assert client is not None
        await client.disconnect()
        returned.set()

    client = await _client(handler, broker)
    broker.push_rx(_auth(0x18))
    await asyncio.wait_for(returned.wait(), 1)
    assert not client.is_connected
    # Reader, effect flush, writer and the AUTH task itself all quiesce.
    await wait_until(lambda: not any(client._running_tasks().values()))


async def test_handler_can_wait_for_a_message_from_the_same_read() -> None:
    # #523: PUBLISH then AUTH in one read. The reader used to hold the earlier
    # message until the AUTH handler had finished.
    broker = _ReauthBroker()
    client: AsyncClient | None = None
    seen = asyncio.Event()

    async def handler(packet: AuthPacket) -> None:
        assert client is not None
        message = await anext(stream)
        assert message.topic == "before/auth"
        seen.set()

    client = await _client(handler, broker)
    stream = client.messages()
    try:
        publish = PublishPacket(
            topic="before/auth", payload=b"x", qos=QoS.AT_MOST_ONCE, retain=False, dup=False
        ).encode(V5)
        broker.push_rx(publish + _auth(0x18))
        await asyncio.wait_for(seen.wait(), 1)
    finally:
        await stream.aclose()
        await client.disconnect()


async def test_handler_can_wait_for_a_receipt_completed_by_the_same_read() -> None:
    # #527: AUTH then the PUBACK of an existing publication in one read.
    broker = _ReauthBroker(auto_puback=False)
    client: AsyncClient | None = None
    settled = asyncio.Event()
    receipt = None

    async def handler(packet: AuthPacket) -> None:
        assert receipt is not None
        await receipt.wait()
        settled.set()

    client = await _client(handler, broker)
    try:
        receipt = await client.publish("pending", b"x", qos=QoS.AT_LEAST_ONCE)
        await wait_until(lambda: broker.published != [])
        broker.push_rx(_auth(0x18) + PubAckPacket(mid=broker.published[0]).encode(V5))
        await asyncio.wait_for(settled.wait(), 1)
    finally:
        await client.disconnect()


async def test_stale_answer_after_broker_disconnect_keeps_the_broker_reason() -> None:
    # #528: AUTH then broker DISCONNECT in one read; the handler's answer is
    # stale and must not replace the broker's verdict.
    broker = _ReauthBroker()
    disconnects: list[BaseException | None] = []

    async def handler(packet: AuthPacket) -> AuthPacket:
        await asyncio.sleep(0)
        return AuthPacket(reason_code=0x18, properties=METHOD)

    client = await _client(handler, broker)
    client.on_disconnect = disconnects.append
    try:
        broker.push_rx(_auth(0x18) + encode_frame(PacketType.DISCONNECT, 0, b"\x8b\x00"))
        await wait_until(lambda: disconnects != [])
        for _ in range(10):
            await asyncio.sleep(0)
        assert isinstance(disconnects[0], BrokerDisconnectError)
        assert disconnects[0].reason_code == 0x8B
        assert broker.client_auth == [0x19]
    finally:
        await client.disconnect()


async def test_answer_to_auth_success_is_ignored() -> None:
    # #535: the broker ended the exchange with Success (0x00); a generic
    # handler answer is not a new Continue and must not break the connection.
    broker = _ReauthBroker()
    calls: list[int] = []

    async def handler(packet: AuthPacket) -> AuthPacket:
        calls.append(packet.reason_code)
        return AuthPacket(reason_code=0x18, properties=METHOD)

    client = await _client(handler, broker)
    try:
        broker.push_rx(_auth(0x00))
        await wait_until(lambda: calls == [0x00])
        for _ in range(10):
            await asyncio.sleep(0)
        assert client.is_connected
        assert broker.client_auth == [0x19]
    finally:
        await client.disconnect()


async def test_handler_never_starts_inside_a_protocol_critical_section(task_factory) -> None:
    # An eager task factory runs a new task synchronously until it suspends;
    # the handler must still start outside the engine and effect locks.
    del task_factory
    broker = _ReauthBroker()
    client: AsyncClient | None = None
    held: list[tuple[bool, bool]] = []

    def handler(packet: AuthPacket) -> AuthPacket:
        assert client is not None
        held.append((client._engine_lock.locked(), client._effect_pump.lock.locked()))
        return AuthPacket(reason_code=0x18, properties=METHOD)

    client = await _client(handler, broker)
    try:
        broker.push_rx(_auth(0x18))
        await wait_until(lambda: broker.client_auth == [0x19, 0x18])
        assert held == [(False, False)]
    finally:
        await client.disconnect()


async def test_invalid_handler_answer_ends_the_connection() -> None:
    # A handler error, including an answer the engine refuses, is the
    # connection's failure rather than a silently dropped exchange.
    broker = _ReauthBroker()
    disconnects: list[BaseException | None] = []

    def handler(packet: AuthPacket) -> AuthPacket:
        return AuthPacket(reason_code=0x00, properties=METHOD)  # a Server code

    client = await _client(handler, broker)
    client.on_disconnect = disconnects.append
    try:
        broker.push_rx(_auth(0x18))
        await wait_until(lambda: disconnects != [])
        assert isinstance(disconnects[0], ProtocolError)
        assert broker.client_auth == [0x19]
    finally:
        await client.disconnect()


async def test_challenge_from_a_retired_connection_is_not_handled() -> None:
    calls: list[AuthPacket] = []

    def handler(packet: AuthPacket) -> None:
        calls.append(packet)

    client = AsyncClient("auth-stale", protocol=V5, auth_handler=handler)
    exchange = client._auth_exchange
    exchange.hand_off(AuthPacket(reason_code=0x18), 1, client._connection_epoch - 1)
    assert exchange.task is not None
    await exchange.task
    assert calls == []


async def test_queued_auth_effect_is_handed_to_the_exchange() -> None:
    # AUTH is applied at collection; the queued branch stays total.
    handed: list[tuple[object, int]] = []
    client = AsyncClient("auth-queued", protocol=V5, auth_handler=lambda packet: None)
    client._auth_exchange.hand_off = lambda packet, token, epoch: handed.append((token, epoch))
    effect = EngineEffect(EffectKind.AUTH, AuthPacket(reason_code=0x18), exchange_token=7)
    await client._apply_effect(effect, nowait=True)
    assert handed == [(7, client._connection_epoch)]
