"""Edge-case coverage for coalesced compatibility publishing."""

from __future__ import annotations

import time
from concurrent.futures import Future

import pytest

import mqttium.compat.paho as paho_compat
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.compat.paho import CallbackAPIVersion, Client
from mqttium.enums import PacketType, QoS
from mqttium.packets import (
    PubAckPacket,
    PubCompPacket,
    PubRecPacket,
    PublishPacket,
    encode_frame,
)
from tests.support import QueueTransport


class _AckingBroker(QueueTransport):
    """Minimal broker stub that completes the QoS 1 and QoS 2 handshakes."""

    def __init__(self) -> None:
        super().__init__()
        self._decoder = IncrementalDecoder()
        self.publishes: list[PublishPacket] = []

    async def write(self, data: bytes) -> None:
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                self.push_rx(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
            elif raw.packet_type is PacketType.PUBLISH:
                publish = PublishPacket.decode(raw.flags, raw.remaining)
                self.publishes.append(publish)
                if publish.mid is None:
                    continue
                if publish.qos is QoS.EXACTLY_ONCE:
                    self.push_rx(PubRecPacket(mid=publish.mid).encode())
                elif publish.qos is QoS.AT_LEAST_ONCE:
                    self.push_rx(PubAckPacket(mid=publish.mid).encode())
            elif raw.packet_type is PacketType.PUBREL:
                mid = int.from_bytes(raw.remaining[:2], "big")
                self.push_rx(PubCompPacket(mid=mid).encode())


def test_oversized_publish_is_processed_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = Client(CallbackAPIVersion.VERSION2, client_id="oversized-batch")
    monkeypatch.setattr(paho_compat, "_PUBLISH_BATCH_MAX_BYTES", 20)
    requests = [
        paho_compat._PendingPublish("small", b"x", False, QoS.AT_MOST_ONCE, None, Future()),
        paho_compat._PendingPublish(
            "oversized", b"x" * 20, False, QoS.AT_MOST_ONCE, None, Future()
        ),
        paho_compat._PendingPublish("tail", b"x", False, QoS.AT_MOST_ONCE, None, Future()),
    ]
    for request in requests:
        client._publish_pending.put(request)

    first, has_more = client._take_publish_batch()
    assert [request.topic for request in first] == ["small"]
    assert has_more

    second, has_more = client._take_publish_batch()
    assert [request.topic for request in second] == ["oversized"]
    assert has_more

    third, has_more = client._take_publish_batch()
    assert [request.topic for request in third] == ["tail"]
    assert not has_more


def test_compat_qos2_publish_completes_full_handshake() -> None:
    broker = _AckingBroker()
    client = Client(CallbackAPIVersion.VERSION2, client_id="compat-qos2")

    async def factory(
        host: str,
        port: int,
        *,
        ssl: object | None = None,
    ) -> _AckingBroker:
        return broker

    client._async._transport_factory = factory
    client.loop_start()
    try:
        assert client.connect("fake", 1883) == 0
        info = client.publish("out/2", b"c", qos=2)
        info.wait_for_publish(timeout=2.0)
        assert info.is_published()
        assert any(
            publish.topic == "out/2" and publish.qos is QoS.EXACTLY_ONCE
            for publish in broker.publishes
        )
    finally:
        client.disconnect()
        client.loop_stop()


def test_on_publish_reports_the_facade_mid_not_the_packet_id() -> None:
    """The callback correlates with the value ``publish()`` handed back.

    The façade counter is advanced first so the two namespaces are observably
    different and a coincidental match cannot pass.
    """
    broker = _AckingBroker()
    client = Client(CallbackAPIVersion.VERSION2, client_id="compat-facade-mid")
    seen: list[int | None] = []
    client.on_publish = lambda _client, _userdata, mid, _rc, _props: seen.append(mid)

    async def factory(
        host: str,
        port: int,
        *,
        ssl: object | None = None,
    ) -> _AckingBroker:
        return broker

    client._async._transport_factory = factory
    client.loop_start()
    try:
        assert client.connect("fake", 1883) == 0
        for _ in range(50):
            client._next_facade_mid()

        infos = [client.publish(f"facade/{index}", b"x", qos=1) for index in range(3)]
        for info in infos:
            info.wait_for_publish(timeout=2.0)

        deadline = time.monotonic() + 2.0
        while len(seen) < 3 and time.monotonic() < deadline:
            time.sleep(0.005)

        facade_mids = [info.mid for info in infos]
        assert facade_mids == [51, 52, 53]
        assert seen == facade_mids
        # What actually went on the wire is the engine's packet identifier.
        wire_mids = {publish.mid for publish in broker.publishes}
        assert wire_mids.isdisjoint(facade_mids)
        assert not client._facade_mid_map
    finally:
        client.disconnect()
        client.loop_stop()
