"""Smoke tests for the Paho VERSION2 compat façade."""

from __future__ import annotations

import asyncio
import threading

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.primitives import pack_u16
from mqttium.compat.paho import CallbackAPIVersion, Client
from mqttium.enums import PacketType, QoS
from mqttium.packets import PubAckPacket, PublishPacket, encode_frame


class FakeBrokerTransport:
    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False

    async def write(self, data: bytes) -> None:
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
            elif raw.packet_type is PacketType.PUBLISH:
                pub = PublishPacket.decode(raw.flags, raw.remaining)
                if pub.qos == QoS.AT_LEAST_ONCE and pub.mid is not None:
                    self._rx.put_nowait(PubAckPacket(mid=pub.mid).encode())
            elif raw.packet_type is PacketType.SUBSCRIBE:
                mid = int.from_bytes(raw.remaining[:2], "big")
                self._rx.put_nowait(encode_frame(PacketType.SUBACK, 0, pack_u16(mid) + bytes([0])))

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        self._closing = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing


def test_compat_connect_publish_qos1() -> None:
    client = Client(CallbackAPIVersion.VERSION2, client_id="compat", userdata={"u": 1})
    fake = FakeBrokerTransport()

    async def factory(host: str, port: int, *, ssl: object = None) -> FakeBrokerTransport:
        return fake

    client._async._transport_factory = factory
    connected = []
    topic_hits = []

    def on_connect(c, userdata, flags, reason_code, properties):
        connected.append((reason_code, userdata))

    def on_topic(c, userdata, message):
        topic_hits.append(message.topic)

    client.on_connect = on_connect
    client.message_callback_add("t/#", on_topic)
    client.loop_start()
    try:
        assert client.connect("fake", 1883) == 0
        assert connected == [(0, {"u": 1})]
        info = client.publish("t/1", b"hi", qos=1)
        info.wait_for_publish(timeout=2.0)
        assert info.is_published()
        rc, mid = client.subscribe("t/#")
        assert rc == 0 and mid > 0
        assert client.is_connected()
        assert client.disconnect() == 0
    finally:
        client.loop_stop()


def test_message_callback_dispatch_only() -> None:
    client = Client(CallbackAPIVersion.VERSION2, userdata="ud")
    seen: list[str] = []
    defaults: list[str] = []

    def on_topic(c, userdata, message):
        seen.append(f"{userdata}:{message.topic}")

    def on_message(c, userdata, message):
        defaults.append(message.topic)

    client.message_callback_add("sensors/+", on_topic)
    client.on_message = on_message
    from mqttium.types import Message

    client._dispatch_message(Message(topic="sensors/1", payload=b"1"))
    client._dispatch_message(Message(topic="other", payload=b"x"))
    assert seen == ["ud:sensors/1"]
    assert defaults == ["other"]


def test_blocking_from_callback_rejected() -> None:
    client = Client(CallbackAPIVersion.VERSION2)
    # Simulate running on the network loop thread inside a user callback.
    client._thread = threading.current_thread()
    client._in_callback = True
    try:
        coro = _noop()
        try:
            client._submit(coro, timeout=0.1)
            raise AssertionError("expected RuntimeError")
        except RuntimeError as exc:
            assert "deadlock" in str(exc).lower() or "callback" in str(exc).lower()
        finally:
            coro.close()  # avoid "never awaited" warning
    finally:
        client._in_callback = False
        client._thread = None


def test_off_loop_qos0_on_publish_keeps_handoff_path() -> None:
    """QoS 0 on_publish fires on the caller thread; blocking calls made from it
    must use the loop handoff, not the on-loop callback fast path."""
    client = Client(CallbackAPIVersion.VERSION2, client_id="compat-race")
    fake = FakeBrokerTransport()

    async def factory(host: str, port: int, *, ssl: object = None) -> FakeBrokerTransport:
        return fake

    client._async._transport_factory = factory
    subscribed = threading.Event()
    errors: list[BaseException] = []

    def on_publish(c, userdata, mid, reason_code, properties):
        try:
            rc, sub_mid = c.subscribe("race/#")
            assert rc == 0 and sub_mid > 0
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)
        finally:
            subscribed.set()

    client.on_publish = on_publish
    client.loop_start()
    try:
        assert client.connect("fake", 1883) == 0
        info = client.publish("race/t", b"x", qos=0)
        info.wait_for_publish(timeout=2.0)
        assert info.is_published()
        assert subscribed.wait(timeout=3.0)
        assert not errors
    finally:
        client.disconnect()
        client.loop_stop()


async def _noop() -> None:
    return None


def test_publish_reports_queue_size_rc_when_admission_is_full() -> None:
    """Engine-side refusal reaches the caller through the publish handle.

    ``publish()`` returns before loop-side admission, so this refusal is
    asynchronous. It must still be observable, and with the same rc and the
    same exceptions as a synchronous ingress refusal.
    """
    client = Client(
        CallbackAPIVersion.VERSION2,
        client_id="queue-full",
        max_pending_outbound_messages=1,
        max_pending_outbound_bytes=None,
    )
    client.loop_start()
    try:
        first = client.publish("queue/first", b"one", qos=1)
        assert first.rc == 0
        assert first.mid is not None

        rejected = client.publish("queue/rejected", b"two", qos=1)
        # Waiting on the handle is what surfaces the refusal; both requests have
        # necessarily been drained by the time it returns.
        with pytest.raises(ValueError, match="ERR_QUEUE_SIZE"):
            rejected.wait_for_publish(timeout=5.0)
        assert rejected.rc == 15
        with pytest.raises(ValueError, match="ERR_QUEUE_SIZE"):
            rejected.is_published()

        # The refused publication consumed no protocol state.
        assert client._async._engine.pending_outbound_messages == 1
        assert len(client._async._engine.packet_ids) == 1
        assert len(client._async._receipts) == 1
    finally:
        client.loop_stop()


def test_publish_returns_queue_size_rc_when_ingress_is_full() -> None:
    """Ingress saturation is still refused synchronously, with mid None."""
    client = Client(
        CallbackAPIVersion.VERSION2,
        client_id="ingress-full",
        max_pending_publish_requests=1,
    )
    client.loop_start()
    try:
        # Block the loop so the first request cannot be drained, then overflow.
        release = threading.Event()
        assert client._loop is not None
        client._loop.call_soon_threadsafe(lambda: release.wait(5.0))
        try:
            accepted = client.publish("ingress/first", b"one", qos=1)
            assert accepted.rc == 0
            rejected = client.publish("ingress/rejected", b"two", qos=1)
            assert rejected.rc == 15
            assert rejected.mid is None
            with pytest.raises(ValueError, match="ERR_QUEUE_SIZE"):
                rejected.wait_for_publish(timeout=0.01)
        finally:
            release.set()
        # The accepted request is still admitted once the loop is released.
        assert accepted._handoff is not None
        accepted._handoff.result(timeout=5.0)
        assert accepted.rc == 0
    finally:
        client.loop_stop()


def test_paho_zero_queue_setting_maps_to_unlimited() -> None:
    client = Client(CallbackAPIVersion.VERSION2, client_id="queue-unlimited")
    client.max_queued_messages_set(0)
    assert client._async._engine.config.max_pending_outbound_messages is None


def test_max_outbound_inflight_is_settable_at_construction() -> None:
    """Attach-time only: EngineConfig refuses it once the engine is attached."""
    client = Client(CallbackAPIVersion.VERSION2, client_id="inflight", max_outbound_inflight=7)
    assert client._async._engine.config.max_outbound_inflight == 7


def test_max_outbound_inflight_defaults_to_broker_receive_maximum() -> None:
    client = Client(CallbackAPIVersion.VERSION2, client_id="inflight-default")
    assert client._async._engine.config.max_outbound_inflight is None


def test_max_outbound_inflight_is_validated() -> None:
    with pytest.raises(ValueError, match="max_outbound_inflight"):
        Client(CallbackAPIVersion.VERSION2, client_id="inflight-bad", max_outbound_inflight=0)
