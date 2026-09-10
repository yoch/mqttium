"""Qualify routed delivery against complete QoS1/QoS2 acknowledgement exchanges."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from mqttium.api import AsyncClient
from mqttium.api._delivery import MessageDelivery
from mqttium.codec.buffer import RawPacket
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, PubRelPacket
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.types import Message
from tests.support import ScriptedBrokerTransport, transport_factory
from tests.unit.test_callback_route_reconfiguration import _callback_errors


class _AckBroker(ScriptedBrokerTransport):
    """Finish inbound QoS2 exchanges and expose actually written terminal ACKs."""

    def __init__(self, protocol: MQTTProtocolVersion, terminal: PacketType, count: int) -> None:
        super().__init__(protocol=protocol)
        self.terminal = terminal
        self.count = count
        self.acks: list[int] = []
        self.recs: list[int] = []
        self.done = asyncio.Event()

    def handle_packet(self, raw: RawPacket) -> None:
        super().handle_packet(raw)
        if raw.packet_type is PacketType.PUBREC:
            mid = int.from_bytes(raw.remaining[:2], "big")
            self.recs.append(mid)
            self.push_rx(PubRelPacket(mid=mid).encode(self.protocol))
        if raw.packet_type is self.terminal:
            self.acks.append(int.from_bytes(raw.remaining[:2], "big"))
            if len(self.acks) == self.count:
                self.done.set()


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
@pytest.mark.parametrize("qos", [1, 2])
@pytest.mark.parametrize("manual", [False, True])
@pytest.mark.parametrize("sqlite", [False, True])
@pytest.mark.parametrize("mode", ["callback", "auto", "both"])
@pytest.mark.parametrize("count", [3, 6])
async def test_route_reconfiguration_preserves_qos_completion(
    protocol: MQTTProtocolVersion,
    qos: int,
    manual: bool,
    sqlite: bool,
    mode: MessageDelivery,
    count: int,
    tmp_path: Path,
) -> None:
    store = SqliteInflightStore(tmp_path / "session.sqlite") if sqlite else None
    transport = _AckBroker(protocol, PacketType.PUBACK if qos == 1 else PacketType.PUBCOMP, count)
    client = AsyncClient(
        client_id="premerge-route",
        protocol=protocol,
        manual_ack=manual,
        store=store,
        message_delivery=mode,
        max_pending_callbacks=8,
        max_pending_messages=8,
        max_pending_delivery_bytes=32768,
    )
    client._transport_factory = transport_factory(transport)
    seen: list[tuple[str, int | None]] = []
    messages: list[Message] = []
    all_seen = asyncio.Event()

    async def replacement(message: Message) -> None:
        await asyncio.sleep(0)  # Real suspension in the changed execution form.
        seen.append(("new", message.mid))
        messages.append(message)
        if len(messages) == count:
            all_seen.set()

    def initial(message: Message) -> None:
        seen.append(("old", message.mid))
        messages.append(message)
        client.message_callback_add("final/#", replacement)

    client.message_callback_add("final/#", initial)
    with _callback_errors() as errors:
        try:
            await asyncio.wait_for(client.connect("unused", 1883), timeout=2)
            transport.push_rx(
                b"".join(
                    PublishPacket(
                        topic="final/x",
                        payload=bytes([mid]) * 512,
                        mid=mid,
                        qos=QoS(qos),
                        retain=False,
                        dup=False,
                    ).encode(protocol)
                    for mid in range(1, count + 1)
                )
            )
            await asyncio.wait_for(all_seen.wait(), timeout=2)
            await asyncio.wait_for(client._callback_queue.join(), timeout=2)
            assert seen == [("old", 1)] + [("new", mid) for mid in range(2, count + 1)]
            if manual:
                assert transport.acks == []
                for message in messages:
                    await client.ack(message)
            await asyncio.wait_for(transport.done.wait(), timeout=2)
            assert transport.acks == list(range(1, count + 1))
            if qos == 2:
                assert transport.recs == list(range(1, count + 1))
            if mode == "both":
                stream = client.messages()
                copies = [await anext(stream) for _ in range(count)]
                assert [message.mid for message in copies] == list(range(1, count + 1))
                await stream.aclose()
            assert client.stats().delivery.pending_bytes == 0
            assert client.stats().delivery.callback_queued == 0
            assert all(client._engine.store.get_in(mid) is None for mid in range(1, count + 1))
            assert client.state is ConnectionState.CONNECTED
            assert errors == []
        finally:
            await asyncio.wait_for(client.disconnect(), timeout=2)
            if store is not None:
                store.close()
