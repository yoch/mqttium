"""Facts the engine has observed never wait behind blocked output.

The writer holds one frame that cannot be written and admits no more, so any
further SEND waits for capacity inside the effect pump. CONNACK, completions
and a broker DISCONNECT observed meanwhile must still reach their waiters.
"""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient, Properties
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import MQTTProtocolVersion, OutboundQoSState, PacketType, QoS
from mqttium.errors import BrokerDisconnectError, ProtocolError
from mqttium.packets import PubAckPacket, PublishPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.types import OutboundMessage
from tests.support import QueueTransport, transport_factory, wait_until


class _GatedBroker(QueueTransport):
    """Writes block while the gate is closed; CONNECT is answered by a script."""

    def __init__(self, protocol: MQTTProtocolVersion, connack: bytes) -> None:
        super().__init__()
        self.protocol = protocol
        self.connack = connack
        self.gate = asyncio.Event()
        self.gate.set()
        self.blocked_on_connect = False
        self.writing = asyncio.Event()
        self.written: list[bytes] = []
        self.decoder = IncrementalDecoder()

    async def write(self, data: bytes) -> None:
        self.writing.set()
        await self.gate.wait()
        self.written.append(data)
        self.decoder.feed(data)
        for raw in self.decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                self.push_rx(self.connack)

    async def write_many(self, parts: list[bytes]) -> None:
        await self.write(b"".join(parts))


def _connack(protocol: MQTTProtocolVersion, *, session_present: bool = False) -> bytes:
    body = bytes((1 if session_present else 0, 0))
    if protocol is MQTTProtocolVersion.MQTTv5:
        body += b"\x00"
    return encode_frame(PacketType.CONNACK, 0, body)


def _publish(mid: int, protocol: MQTTProtocolVersion) -> bytes:
    return PublishPacket(
        topic=f"in/{mid}", payload=b"x", qos=QoS.AT_LEAST_ONCE, retain=False, dup=False, mid=mid
    ).encode(protocol)


async def _block_writer(client: AsyncClient, broker: _GatedBroker, protocol) -> None:
    """Leave one PUBACK stuck in the writer; the next frame waits for capacity."""
    broker.gate.clear()
    broker.writing.clear()
    broker.push_rx(_publish(100, protocol))
    await asyncio.wait_for(broker.writing.wait(), 1)


async def test_observed_puback_settles_its_receipt_behind_blocked_output() -> None:
    # #532
    protocol = MQTTProtocolVersion.MQTTv311
    broker = _GatedBroker(protocol, _connack(protocol))
    client = AsyncClient("puback-backpressure", max_write_queue_messages=1, keepalive=0)
    client._transport_factory = transport_factory(broker)
    await client.connect("fake")
    try:
        receipt = await client.publish("out", b"x", qos=QoS.AT_LEAST_ONCE)
        assert receipt.mid is not None
        await wait_until(lambda: len(broker.written) == 2)  # CONNECT, PUBLISH
        await _block_writer(client, broker, protocol)
        # The PUBACK is decoded in the same lot as a PUBLISH whose own PUBACK
        # then waits for writer capacity (the reader cannot observe an ACK it
        # has not read, so the ACK comes first on the wire).
        broker.push_rx(PubAckPacket(mid=receipt.mid).encode(protocol) + _publish(101, protocol))
        await wait_until(lambda: client._write_pump.waiters == 1)
        await asyncio.wait_for(receipt.wait(), 1)
    finally:
        broker.gate.set()
        await client.disconnect()


async def test_broker_disconnect_is_not_hidden_behind_blocked_output() -> None:
    # #531
    protocol = MQTTProtocolVersion.MQTTv5
    broker = _GatedBroker(protocol, _connack(protocol))
    disconnects: list[BaseException | None] = []
    client = AsyncClient(
        "disconnect-backpressure", protocol=protocol, max_write_queue_messages=1, keepalive=0
    )
    client.on_disconnect = disconnects.append
    client._transport_factory = transport_factory(broker)
    await client.connect("fake")
    try:
        await _block_writer(client, broker, protocol)
        broker.push_rx(
            _publish(101, protocol) + encode_frame(PacketType.DISCONNECT, 0, b"\x8b\x00")
        )
        await wait_until(lambda: disconnects != [])
        assert isinstance(disconnects[0], BrokerDisconnectError)
        assert disconnects[0].reason_code == 0x8B
    finally:
        broker.gate.set()
        await client.disconnect()


async def test_connack_is_not_hidden_behind_blocked_session_replay() -> None:
    # #536: session replay SENDs precede CONNACK in the same effect batch.
    protocol = MQTTProtocolVersion.MQTTv311
    store = MemoryInflightStore()
    for mid in (1, 2, 3):
        store.put_out(
            OutboundMessage(
                mid=mid,
                topic="replayed",
                payload=b"x",
                qos=QoS.AT_LEAST_ONCE,
                retain=False,
                state=OutboundQoSState.WAIT_PUBACK,
                logical_size=18,
            )
        )
    broker = _GatedBroker(protocol, _connack(protocol, session_present=True))
    client = AsyncClient(
        "connack-backpressure",
        store=store,
        clean_start=False,
        max_write_queue_messages=1,
        keepalive=0,
    )
    client._transport_factory = transport_factory(broker)

    async def close_gate_after_connect() -> None:
        await wait_until(lambda: len(broker.written) == 1)
        broker.gate.clear()

    closer = asyncio.create_task(close_gate_after_connect())
    try:
        connack = await asyncio.wait_for(client.connect("fake", timeout=1), 2)
        assert connack.session_present
    finally:
        broker.gate.set()
        await closer
        await client.disconnect()


async def test_invalid_connack_fails_connect_behind_blocked_output() -> None:
    # #540: the engine's normative DISCONNECT waits for capacity; connect()
    # must report the protocol error, not a CONNACK timeout.
    protocol = MQTTProtocolVersion.MQTTv5
    props = encode_properties(Properties({"authentication_method": "unexpected"}), "CONNACK")
    broker = _GatedBroker(protocol, b"")
    client = AsyncClient(
        "invalid-connack", protocol=protocol, max_write_queue_messages=1, keepalive=0
    )
    client._transport_factory = transport_factory(broker)
    broker.gate.clear()  # CONNECT stays in the writer

    async def answer() -> None:
        await asyncio.wait_for(broker.writing.wait(), 1)
        broker.push_rx(encode_frame(PacketType.CONNACK, 0, b"\x00\x00" + props))

    responder = asyncio.create_task(answer())
    try:
        with pytest.raises(ProtocolError):
            await asyncio.wait_for(client.connect("fake", timeout=1), 2)
    finally:
        broker.gate.set()
        await responder
        await client.disconnect()


async def test_broker_disconnect_fails_application_output_parked_for_capacity() -> None:
    # #531 through the application: a publish parked for writer capacity is
    # part of the lot's protocol target; the broker's verdict must fail it
    # rather than leave the reader waiting behind it.
    protocol = MQTTProtocolVersion.MQTTv5
    broker = _GatedBroker(protocol, _connack(protocol))
    disconnects: list[BaseException | None] = []
    client = AsyncClient(
        "disconnect-parked", protocol=protocol, max_write_queue_messages=1, keepalive=0
    )
    client.on_disconnect = disconnects.append
    client._transport_factory = transport_factory(broker)
    await client.connect("fake")
    parked = None
    try:
        await _block_writer(client, broker, protocol)
        parked = asyncio.create_task(client.publish("parked", b"x", qos=QoS.AT_MOST_ONCE))
        await wait_until(lambda: client._write_pump.waiters == 1)
        broker.push_rx(encode_frame(PacketType.DISCONNECT, 0, b"\x8b\x00"))
        await wait_until(lambda: disconnects != [])
        assert isinstance(disconnects[0], BrokerDisconnectError)
        await asyncio.wait_for(asyncio.gather(parked, return_exceptions=True), 1)
    finally:
        broker.gate.set()
        if parked is not None:
            parked.cancel()
            await asyncio.gather(parked, return_exceptions=True)
        await client.disconnect()
