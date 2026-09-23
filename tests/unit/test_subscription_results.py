"""subscribe()/unsubscribe() return the public result of their own acknowledgement."""

import asyncio

import pytest

from mqttium.api import AsyncClient, SubscribeResult, UnsubscribeResult
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.packets import encode_frame
from tests.support import ScriptedBrokerTransport, transport_factory, wait_until

V311 = MQTTProtocolVersion.MQTTv311
V5 = MQTTProtocolVersion.MQTTv5

# Per protocol and operation: reason codes for a three-topic and a two-topic
# request, including failure codes, in the order the broker returns them.
_CODES = {
    (V311, False): ((0x00, 0x80, 0x02), (0x80, 0x01)),
    (V5, False): ((0x00, 0x87, 0x02), (0x91, 0x01)),
    (V311, True): ((), ()),
    (V5, True): ((0x00, 0x11, 0x80), (0x87, 0x00)),
}


class _HeldAckBroker(ScriptedBrokerTransport):
    """Record requests and acknowledge them only when the test says so."""

    def __init__(self, protocol):
        super().__init__(protocol=protocol)
        self.requests: list[int] = []

    def handle_packet(self, raw):
        if raw.packet_type in (PacketType.SUBSCRIBE, PacketType.UNSUBSCRIBE):
            self.requests.append(int.from_bytes(raw.remaining[:2], "big"))
        else:
            super().handle_packet(raw)

    def ack(self, mid, unsubscribe, codes):
        properties = b"\x00" if self.protocol is V5 else b""
        kind = PacketType.UNSUBACK if unsubscribe else PacketType.SUBACK
        self.push_rx(encode_frame(kind, 0, mid.to_bytes(2, "big") + properties + bytes(codes)))


@pytest.mark.parametrize("protocol", [V311, V5])
@pytest.mark.parametrize("unsubscribe", [False, True])
async def test_each_request_receives_its_own_ordered_reason_codes(protocol, unsubscribe):
    broker = _HeldAckBroker(protocol)
    client = AsyncClient("results", protocol=protocol, keepalive=0)
    client._transport_factory = transport_factory(broker)
    request = client.unsubscribe if unsubscribe else client.subscribe
    model = UnsubscribeResult if unsubscribe else SubscribeResult
    futures = client._unsub_futs if unsubscribe else client._sub_futs
    first_codes, second_codes = _CODES[protocol, unsubscribe]
    await client.connect("unused")
    tasks = []
    try:
        tasks.append(asyncio.create_task(request(["a/1", "a/2", "a/3"])))
        await wait_until(lambda: len(broker.requests) == 1)
        tasks.append(asyncio.create_task(request(["b/1", "b/2"])))
        await wait_until(lambda: len(broker.requests) == 2)
        first_mid, second_mid = broker.requests
        assert first_mid != second_mid

        # Acknowledge in reverse order: each future keeps its own identifier.
        broker.ack(second_mid, unsubscribe, second_codes)
        second = await asyncio.wait_for(tasks[1], 1)
        assert not tasks[0].done()
        broker.ack(first_mid, unsubscribe, first_codes)
        first = await asyncio.wait_for(tasks[0], 1)

        assert type(first) is model and type(second) is model
        assert (first.mid, first.reason_codes) == (first_mid, first_codes)
        assert (second.mid, second.reason_codes) == (second_mid, second_codes)
        assert not futures
        for mid in (first_mid, second_mid):
            assert not client._engine.outbound.packet_ids.in_use(mid)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await client.disconnect()
