"""Inbound protocol completion waits for the application to own the message.

A persisted QoS 2 row (or a recovered QoS 1 row acknowledged automatically) is
the last durable copy of the message. PUBCOMP/PUBACK and the row deletion hand
ownership away, so they follow the delivery commit, never just the emission of
the MESSAGE effect. Delivery marks carry the exchange identity so a late mark
cannot reach a later exchange that reuses the packet identifier.
"""

from __future__ import annotations

import asyncio

from mqttium.api import AsyncClient
from mqttium.enums import InboundQoSState, PacketType, QoS
from mqttium.packets import PublishPacket, PubRelPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.protocol.inbound import REPLAY_BATCH_MESSAGES
from mqttium.types import InboundMessage
from tests.support import ScriptedBrokerTransport, feed_engine, transport_factory, wait_until


def _publish(mid: int, qos: QoS, payload: bytes, *, dup: bool = False) -> bytes:
    return PublishPacket(
        topic=f"commit/{mid}",
        payload=payload,
        qos=qos,
        retain=False,
        dup=dup,
        mid=mid,
    ).encode()


def _written_acks(transport: ScriptedBrokerTransport, packet_type: PacketType) -> list[int]:
    mids = []
    for data in transport.written:
        if data and data[0] & 0xF0 == int(packet_type) and len(data) >= 4:
            mids.append(int.from_bytes(data[2:4], "big"))
    return mids


class _ResumingBroker(ScriptedBrokerTransport):
    """CONNACK with Session Present, optionally coalesced with more packets."""

    def __init__(self, after_connack: bytes = b"") -> None:
        super().__init__()
        self.after_connack = after_connack

    def handle_packet(self, raw) -> None:
        if raw.packet_type is PacketType.CONNECT:
            self.push_rx(encode_frame(PacketType.CONNACK, 0, b"\x01\x00") + self.after_connack)
            return
        super().handle_packet(raw)


async def test_pubrel_in_the_same_read_waits_for_application_ownership() -> None:
    # #520: PUBLISH and PUBREL arrive together while the iterator is full.
    # PUBCOMP and row deletion before the message is owned lose it for good.
    store = MemoryInflightStore()
    transport = ScriptedBrokerTransport()
    client = AsyncClient("qos2-commit", store=store, max_iterator_messages=1, keepalive=0)
    client._transport_factory = transport_factory(transport)
    await client.connect("fake")
    stream = client.messages()
    try:
        transport.push_rx(_publish(1, QoS.EXACTLY_ONCE, b"fills-queue"))
        await wait_until(lambda: client._delivery.messages_queue.full())
        transport.push_rx(_publish(2, QoS.EXACTLY_ONCE, b"held") + PubRelPacket(mid=2).encode())
        await wait_until(lambda: 2 in _written_acks(transport, PacketType.PUBREC))
        for _ in range(10):
            await asyncio.sleep(0)

        assert 2 not in _written_acks(transport, PacketType.PUBCOMP)
        assert store.get_in(2) is not None

        first = await asyncio.wait_for(anext(stream), 1)
        second = await asyncio.wait_for(anext(stream), 1)
        assert (first.mid, second.mid) == (1, 2)
        await wait_until(lambda: 2 in _written_acks(transport, PacketType.PUBCOMP))
        assert store.get_in(2) is None
    finally:
        await stream.aclose()
        await client.disconnect()


async def test_recovered_qos1_on_a_later_replay_page_is_not_lost() -> None:
    # #519: manual QoS 1 rows resumed by an auto-acknowledging client. The
    # broker retransmits the last one in the same read as CONNACK, before the
    # bounded replay has reached its page: acknowledging and deleting it then
    # loses the message on a healthy connection.
    store = MemoryInflightStore()
    last = REPLAY_BATCH_MESSAGES + 1
    for mid in range(1, last + 1):
        store.put_in(
            InboundMessage(
                mid=mid,
                topic=f"recovered/{mid}",
                payload=b"body",
                qos=QoS.AT_LEAST_ONCE,
                retain=False,
                state=InboundQoSState.WAIT_PUBACK,
                delivered=False,
                logical_size=18,
            )
        )
    transport = _ResumingBroker(after_connack=_publish(last, QoS.AT_LEAST_ONCE, b"body", dup=True))
    client = AsyncClient("recovered-qos1", store=store, clean_start=False, keepalive=0)
    client._transport_factory = transport_factory(transport)
    await client.connect("fake")
    stream = client.messages()
    try:
        mids = [(await asyncio.wait_for(anext(stream), 1)).mid for _ in range(last)]
        assert sorted(mids) == list(range(1, last + 1))
        await wait_until(lambda: last in _written_acks(transport, PacketType.PUBACK))
        assert store.get_in(last) is None
    finally:
        await stream.aclose()
        await client.disconnect()


def test_late_delivery_mark_cannot_reach_a_reused_packet_identifier() -> None:
    # #534: the mark of a completed exchange arrives after the broker legally
    # reused its identifier. Marking the new row would skip it on replay.
    engine = ProtocolEngine(EngineConfig(client_id="reuse", manual_ack=True))
    engine.begin_connect()
    feed_engine(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
    engine.take_effects()

    feed_engine(engine, _publish(5, QoS.AT_LEAST_ONCE, b"old"))
    old = [e for e in engine.take_effects() if e.kind is EffectKind.MESSAGE][0]
    engine.ack(5)
    engine.take_effects()
    feed_engine(engine, _publish(5, QoS.AT_LEAST_ONCE, b"new"))
    engine.take_effects()

    engine.mark_inbound_delivered(5, old.exchange_token)

    row = engine.store.get_in(5)
    assert row is not None and row.payload == b"new"
    assert row.delivered is False


async def test_callback_delivery_is_marked_before_the_fairness_yield() -> None:
    # #517: the callback already owns the message; a mark deferred behind the
    # fairness yield is lost if the reader is cancelled at that yield.
    store = MemoryInflightStore()
    transport = ScriptedBrokerTransport()
    received: list[int | None] = []
    client = AsyncClient("callback-commit", store=store, message_delivery="callback", keepalive=0)
    client.on_message = lambda message: received.append(message.mid)
    client._transport_factory = transport_factory(transport)
    await client.connect("fake")
    try:
        client._delivery._since_yield = 10**6  # the next message ends a quantum
        client._engine.handle_raw(_raw(_publish(3, QoS.EXACTLY_ONCE, b"x")))
        effect = [e for e in client._engine.take_effects() if e.kind is EffectKind.MESSAGE][0]

        pending = client._apply_delivery_effect(effect, client._connection_epoch)

        assert received == [3]
        assert pending is not None  # the fairness yield
        row = store.get_in(3)
        assert row is not None and row.delivered is True
        await pending
    finally:
        await client.disconnect()


def _raw(wire: bytes):
    from mqttium.codec.buffer import IncrementalDecoder

    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    return raw
