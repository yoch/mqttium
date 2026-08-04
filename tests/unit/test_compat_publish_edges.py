"""Edge-case coverage for coalesced compatibility publishing."""

from __future__ import annotations

import pytest

import mqttium.compat.paho as paho_compat
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.compat.paho import CallbackAPIVersion, Client
from mqttium.enums import PacketType, QoS
from mqttium.packets import PubCompPacket, PubRecPacket, PublishPacket, encode_frame
from tests.support import QueueTransport


class _QoS2Broker(QueueTransport):
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
                if publish.qos is QoS.EXACTLY_ONCE and publish.mid is not None:
                    self.push_rx(PubRecPacket(mid=publish.mid).encode())
            elif raw.packet_type is PacketType.PUBREL:
                mid = int.from_bytes(raw.remaining[:2], "big")
                self.push_rx(PubCompPacket(mid=mid).encode())


def test_oversized_publish_is_processed_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = Client(CallbackAPIVersion.VERSION2, client_id="oversized-batch")
    monkeypatch.setattr(paho_compat, "_PUBLISH_BATCH_MAX_BYTES", 20)
    requests = [
        paho_compat._PendingPublish("small", b"x", False, QoS.AT_MOST_ONCE, None),
        paho_compat._PendingPublish("oversized", b"x" * 20, False, QoS.AT_MOST_ONCE, None),
        paho_compat._PendingPublish("tail", b"x", False, QoS.AT_MOST_ONCE, None),
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
    broker = _QoS2Broker()
    client = Client(CallbackAPIVersion.VERSION2, client_id="compat-qos2")

    async def factory(
        host: str,
        port: int,
        *,
        ssl: object | None = None,
    ) -> _QoS2Broker:
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
