"""Behavioral subset of Paho's tests/lib exercised through VERSION2 compatibility.

These tests do not replay Paho's C broker simulator. They cover the VERSION2
surface documented in ``docs/paho-compatibility.md`` with an in-memory transport,
mirroring these ``tests/lib`` scenarios:

- ``01-unpwd-set`` / will / clean session
- ``02-subscribe-qos0/1/2``
- ``03-publish-qos0`` / qos1 wait
- ``04-retain-qos0``
"""

from __future__ import annotations


from tests.support import QueueTransport

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.primitives import pack_u16
from mqttium.compat.paho import CallbackAPIVersion, Client
from mqttium.enums import PacketType, QoS
from mqttium.packets import PubAckPacket, PublishPacket, encode_frame
from mqttium.types import Message


class FakeBrokerTransport(QueueTransport):
    def __init__(self) -> None:
        super().__init__()
        self._decoder = IncrementalDecoder()
        self.connect_seen: bytes | None = None
        self.publishes: list[PublishPacket] = []
        self.subscribes: list[bytes] = []

    async def write(self, data: bytes) -> None:
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                self.connect_seen = raw.remaining
                self.push_rx(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
            elif raw.packet_type is PacketType.PUBLISH:
                pub = PublishPacket.decode(raw.flags, raw.remaining)
                self.publishes.append(pub)
                if pub.qos == QoS.AT_LEAST_ONCE and pub.mid is not None:
                    self.push_rx(PubAckPacket(mid=pub.mid).encode())
                elif pub.qos == QoS.EXACTLY_ONCE and pub.mid is not None:
                    # Minimal QoS2: PUBREC → expect PUBREL → PUBCOMP (engine side).
                    from mqttium.packets import PubRecPacket

                    self.push_rx(PubRecPacket(mid=pub.mid).encode())
            elif raw.packet_type is PacketType.PUBREL:
                from mqttium.packets import PubCompPacket

                mid = int.from_bytes(raw.remaining[:2], "big")
                self.push_rx(PubCompPacket(mid=mid).encode())
            elif raw.packet_type is PacketType.SUBSCRIBE:
                self.subscribes.append(raw.remaining)
                mid = int.from_bytes(raw.remaining[:2], "big")
                self.push_rx(encode_frame(PacketType.SUBACK, 0, pack_u16(mid) + bytes([0])))
            elif raw.packet_type is PacketType.UNSUBSCRIBE:
                mid = int.from_bytes(raw.remaining[:2], "big")
                self.push_rx(encode_frame(PacketType.UNSUBACK, 0, pack_u16(mid)))

    def push_publish(
        self, topic: str, payload: bytes, *, qos: int = 0, retain: bool = False
    ) -> None:
        mid = 1 if qos else None
        pkt = PublishPacket(
            topic=topic,
            payload=payload,
            qos=QoS(qos),
            retain=retain,
            dup=False,
            mid=mid,
        )
        self.push_rx(pkt.encode())


def _client_with_fake(fake: FakeBrokerTransport, **kwargs) -> Client:
    client = Client(CallbackAPIVersion.VERSION2, **kwargs)

    async def factory(host: str, port: int, *, ssl: object = None) -> FakeBrokerTransport:
        return fake

    client._async._transport_factory = factory
    return client


def test_lib_01_username_password_and_will() -> None:
    fake = FakeBrokerTransport()
    client = _client_with_fake(fake, client_id="lib01")
    client.username_pw_set("user", "secret")
    client.will_set("will/t", payload=b"bye", qos=1, retain=True)
    client.loop_start()
    try:
        assert client.connect("fake", 1883) == 0
        assert fake.connect_seen is not None
        # Username flag / will flag present in CONNECT flags byte (MQTT 3.1.1).
        # remaining: proto name, level, flags, keepalive, client_id, …
        body = fake.connect_seen
        # Skip protocol name length+name + level → flags at fixed offset for "MQTT"/4.
        assert body[0:6] == b"\x00\x04MQTT"
        flags = body[7]
        assert flags & 0x80  # username
        assert flags & 0x40  # password
        assert flags & 0x04  # will
        assert flags & 0x20  # will retain
        assert ((flags >> 3) & 0x03) == 1  # will qos 1
    finally:
        client.disconnect()
        client.loop_stop()


def test_lib_02_subscribe_qos_levels() -> None:
    fake = FakeBrokerTransport()
    client = _client_with_fake(fake, client_id="lib02")
    client.loop_start()
    try:
        assert client.connect("fake", 1883) == 0
        for qos in (0, 1, 2):
            rc, mid = client.subscribe(f"t/{qos}", qos=qos)
            assert rc == 0 and mid > 0
        # subscribe() is fire-and-forget (Paho semantics): allow the loop to
        # flush the SUBSCRIBE frames before asserting.
        import time

        deadline = time.monotonic() + 2.0
        while len(fake.subscribes) < 3 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(fake.subscribes) == 3
    finally:
        client.disconnect()
        client.loop_stop()


def test_lib_03_publish_qos0_and_qos1() -> None:
    fake = FakeBrokerTransport()
    client = _client_with_fake(fake, client_id="lib03")
    client.loop_start()
    try:
        assert client.connect("fake", 1883) == 0
        info0 = client.publish("out/0", b"a", qos=0)
        info0.wait_for_publish(timeout=2.0)
        info1 = client.publish("out/1", b"b", qos=1)
        info1.wait_for_publish(timeout=2.0)
        assert info1.is_published()
        topics = [p.topic for p in fake.publishes]
        assert "out/0" in topics and "out/1" in topics
    finally:
        client.disconnect()
        client.loop_stop()


def test_lib_04_retain_and_inbound_message() -> None:
    fake = FakeBrokerTransport()
    client = _client_with_fake(fake, client_id="lib04")
    seen: list[Message] = []

    def on_message(c, userdata, message):
        seen.append(message)

    client.on_message = on_message
    client.loop_start()
    try:
        assert client.connect("fake", 1883) == 0
        client.publish("ret/t", b"r", qos=0, retain=True)
        # Allow writer to flush.
        import time

        time.sleep(0.05)
        assert any(p.retain for p in fake.publishes)
        fake.push_publish("in/t", b"hello", qos=0, retain=False)
        deadline = time.monotonic() + 2.0
        while not seen and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(seen) == 1
        assert seen[0].topic == "in/t"
        assert seen[0].payload == b"hello"
    finally:
        client.disconnect()
        client.loop_stop()
