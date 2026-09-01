"""Native and façade callback publication preserve QoS completion semantics."""

from __future__ import annotations

import asyncio
import threading
import time

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.api import AsyncClient
from mqttium.compat.paho import CallbackAPIVersion, Client, MQTTMessageInfo
from mqttium.enums import PacketType, QoS
from mqttium.packets import PubAckPacket, PublishPacket, encode_frame


class FakeBrokerTransport:
    def __init__(self, *, auto_ack: bool = True) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False
        self.auto_ack = auto_ack
        self.published: list[PublishPacket] = []
        self.pending_pubacks: list[int] = []

    async def write(self, data: bytes) -> None:
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
            elif raw.packet_type is PacketType.PUBLISH:
                pub = PublishPacket.decode(raw.flags, raw.remaining)
                self.published.append(pub)
                if pub.qos == QoS.AT_LEAST_ONCE and pub.mid is not None:
                    if self.auto_ack:
                        self._rx.put_nowait(PubAckPacket(mid=pub.mid).encode())
                    else:
                        self.pending_pubacks.append(pub.mid)

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        self._closing = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing

    def push_publish(
        self,
        topic: str,
        payload: bytes,
        *,
        qos: QoS = QoS.AT_MOST_ONCE,
        mid: int | None = None,
    ) -> None:
        pkt = PublishPacket(
            topic=topic,
            payload=payload,
            qos=qos,
            retain=False,
            dup=False,
            mid=mid,
        )
        self._rx.put_nowait(pkt.encode())

    def ack_pending(self) -> None:
        for mid in self.pending_pubacks:
            self._rx.put_nowait(PubAckPacket(mid=mid).encode())
        self.pending_pubacks.clear()


def test_publish_from_on_message_waits_for_puback() -> None:
    fake = FakeBrokerTransport(auto_ack=False)
    client = Client(CallbackAPIVersion.VERSION2, client_id="cb-pub")

    async def factory(host: str, port: int, *, ssl: object = None) -> FakeBrokerTransport:
        return fake

    client._async._transport_factory = factory
    queued = threading.Event()
    published_callback = threading.Event()
    errors: list[BaseException] = []
    info_box: dict[str, MQTTMessageInfo] = {}

    def on_publish(c, userdata, mid, reason_code, properties):
        published_callback.set()

    def on_message(c, userdata, message):
        try:
            info = c.publish("out/echo", b"reply:" + message.payload, qos=1)
            assert info.mid is not None
            info_box["info"] = info
            queued.set()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)
            queued.set()

    client.on_message = on_message
    client.on_publish = on_publish
    client.loop_start()
    try:
        assert client.connect("fake", 1883) == 0
        assert client._loop is not None
        client._loop.call_soon_threadsafe(fake.push_publish, "in/t", b"hello")

        assert queued.wait(timeout=3.0), "callback publish deadlocked"
        assert not errors
        info = info_box["info"]

        deadline = time.monotonic() + 2.0
        while not fake.pending_pubacks and time.monotonic() < deadline:
            time.sleep(0.01)
        assert fake.pending_pubacks, "QoS 1 publish did not reach the broker"

        # Regression: the old callback path used _event=None and reported the
        # publish complete immediately, before PUBACK.
        assert not info.is_published()
        assert not published_callback.is_set()

        client._loop.call_soon_threadsafe(fake.ack_pending)
        info.wait_for_publish(timeout=2.0)
        assert info.is_published()
        assert published_callback.wait(timeout=2.0)
    finally:
        client.disconnect()
        client.loop_stop()


async def test_native_inline_on_message_can_publish_qos1_reentrantly() -> None:
    fake = FakeBrokerTransport(auto_ack=True)
    client = AsyncClient(client_id="native-cb-pub", message_delivery="callback")

    async def factory(host: str, port: int, *, ssl: object = None) -> FakeBrokerTransport:
        return fake

    client._transport_factory = factory  # type: ignore[assignment]
    delivered = asyncio.Event()
    receipts = []

    def on_message(message) -> None:
        assert client._delivery._callback_active is True
        receipts.append(client.publish_nowait("out/echo", b"reply:" + message.payload, qos=1))
        delivered.set()

    client.on_message = on_message
    await client.connect("fake", 1883)
    try:
        fake.push_publish("in/t", b"hello", qos=QoS.AT_LEAST_ONCE, mid=41)

        await asyncio.wait_for(delivered.wait(), timeout=1.0)
        assert len(receipts) == 1
        await asyncio.wait_for(receipts[0].wait(), timeout=1.0)
        assert [packet.payload for packet in fake.published] == [b"reply:hello"]
    finally:
        await client.disconnect()


def test_inner_publish_callback_tracks_the_facade_callback() -> None:
    """The native direct path retains callback-worker isolation for the façade."""
    client = Client(CallbackAPIVersion.VERSION2, client_id="cb-gate")

    assert client.on_publish is None
    assert client._async.on_publish is None
    assert client._async._direct_qos0_ready() is True

    def on_publish(c, userdata, mid, reason_code, properties):
        return None

    client.on_publish = on_publish
    assert client.on_publish is on_publish
    assert client._async.on_publish is not None
    assert client._async._direct_qos0_ready() is True

    client.on_publish = None
    assert client._async.on_publish is None
    assert client._async._direct_qos0_ready() is True


def test_publish_without_callback_skips_the_callback_queue() -> None:
    """Completion settles inline when no façade callback wants it."""
    fake = FakeBrokerTransport(auto_ack=True)
    client = Client(CallbackAPIVersion.VERSION2, client_id="cb-inline")

    async def factory(host: str, port: int, *, ssl: object = None) -> FakeBrokerTransport:
        return fake

    client._async._transport_factory = factory
    enqueued: list[object] = []
    original = client._async._enqueue_callback

    async def counting_enqueue(callback, *args):
        enqueued.append(callback)
        return await original(callback, *args)

    client._async._enqueue_callback = counting_enqueue  # type: ignore[method-assign]
    client.loop_start()
    try:
        assert client.connect("fake", 1883) == 0
        for _ in range(20):
            info = client.publish("out/inline", b"x", qos=1)
            info.wait_for_publish(timeout=5)
        deadline = time.monotonic() + 5
        while len(fake.published) < 20 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(fake.published) == 20
        publish_jobs = [cb for cb in enqueued if cb == client._dispatch_publish]
        assert publish_jobs == []
    finally:
        client.disconnect()
        client.loop_stop()


def test_setting_the_callback_from_another_thread_is_applied() -> None:
    fake = FakeBrokerTransport(auto_ack=True)
    client = Client(CallbackAPIVersion.VERSION2, client_id="cb-thread")

    async def factory(host: str, port: int, *, ssl: object = None) -> FakeBrokerTransport:
        return fake

    client._async._transport_factory = factory
    client.loop_start()
    try:
        assert client.connect("fake", 1883) == 0
        seen = threading.Event()

        def on_publish(c, userdata, mid, reason_code, properties):
            seen.set()

        setter = threading.Thread(target=lambda: setattr(client, "on_publish", on_publish))
        setter.start()
        setter.join(5)

        assert client._async.on_publish is not None
        info = client.publish("out/thread", b"x", qos=1)
        info.wait_for_publish(timeout=5)
        assert seen.wait(5)
    finally:
        client.disconnect()
        client.loop_stop()
