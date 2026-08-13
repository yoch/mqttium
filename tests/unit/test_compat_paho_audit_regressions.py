"""Regressions for the RC2 audit findings in the Paho compatibility façade."""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import pytest

import mqttium.compat.paho as paho
from mqttium.api.models import PublishReceipt
from mqttium.compat.paho import Client, ConnectFlags, MQTTMessageInfo
from mqttium.enums import QoS
from mqttium.errors import ProtocolError
from mqttium.packets import ConnAckPacket


def test_loop_start_serializes_first_thread_creation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Concurrent first users must create exactly one owned network thread."""
    client = Client()
    real_thread = threading.Thread
    constructor_lock = threading.Lock()
    barrier = threading.Barrier(8)
    constructor_calls = 0

    class FakeNetworkThread:
        def __init__(self) -> None:
            self._alive = False

        def start(self) -> None:
            self._alive = True
            client._started.set()

        def is_alive(self) -> bool:
            return self._alive

        def join(self, timeout: float | None = None) -> None:
            del timeout
            self._alive = False

    def make_thread(*args: Any, **kwargs: Any) -> FakeNetworkThread:
        del args, kwargs
        nonlocal constructor_calls
        with constructor_lock:
            constructor_calls += 1
        # Release the GIL in the exact window that used to sit between the
        # unlocked is_alive() guard and self._thread assignment.
        time.sleep(0.02)
        return FakeNetworkThread()

    monkeypatch.setattr(paho.threading, "Thread", make_thread)

    def start_together() -> None:
        barrier.wait()
        client.loop_start()

    callers = [real_thread(target=start_together) for _ in range(barrier.parties)]
    for caller in callers:
        caller.start()
    for caller in callers:
        caller.join(timeout=2.0)
        assert not caller.is_alive()

    assert constructor_calls == 1


@pytest.mark.asyncio
async def test_wait_for_publish_rejects_network_event_loop() -> None:
    loop = asyncio.get_running_loop()
    receipt = PublishReceipt(mid=1, qos=QoS.AT_LEAST_ONCE)
    info = MQTTMessageInfo(mid=1, _receipt=receipt, _loop=loop)

    with pytest.raises(RuntimeError, match="network event loop"):
        info.wait_for_publish(timeout=0.01)


def test_qos0_admission_error_reaches_publish_handle() -> None:
    client = Client()
    client.loop_start()
    try:
        info = client.publish("invalid/+topic", b"x", qos=0)
        with pytest.raises(ProtocolError):
            info.wait_for_publish(timeout=2.0)
        with pytest.raises(ProtocolError):
            info.is_published()
    finally:
        client.loop_stop()


def test_publish_none_is_zero_length_payload() -> None:
    client = Client()
    seen: list[bytes] = []

    def queue_qos0(topic: str, payload: bytes, *, retain: bool) -> None:
        del topic, retain
        seen.append(payload)

    client.loop_start()
    try:
        client._queue_qos0_on_loop = queue_qos0
        client._finalize_publish_effects = lambda: None
        info = client.publish("retain/clear", None, qos=0, retain=True)
        info.wait_for_publish(timeout=2.0)
        assert seen == [b""]
    finally:
        client.loop_stop()


def test_is_connected_matches_paho_method_shape() -> None:
    client = Client()
    assert callable(client.is_connected)
    assert client.is_connected() is False


def test_connect_callback_receives_connect_flags_object() -> None:
    client = Client()
    seen: list[ConnectFlags] = []

    def on_connect(
        c: Client, userdata: object, flags: ConnectFlags, reason: int, props: object
    ) -> None:
        del c, userdata, reason, props
        seen.append(flags)

    client.on_connect = on_connect
    client._dispatch_connect(ConnAckPacket(session_present=True, reason_code=0))

    assert seen == [ConnectFlags(session_present=True)]
    assert seen[0].session_present is True


def test_message_callback_add_rejects_invalid_filter() -> None:
    client = Client()
    with pytest.raises(ProtocolError):
        client.message_callback_add("sport/#/ranking", lambda *args: None)


def test_disconnect_from_network_thread_raises_before_creating_coroutine() -> None:
    client = Client()
    client._thread = threading.current_thread()
    try:
        with pytest.raises(RuntimeError, match="network event loop"):
            client.disconnect()
    finally:
        client._thread = None


def test_loop_stop_from_network_thread_does_not_join_itself() -> None:
    client = Client()
    client.loop_start()
    done = threading.Event()
    errors: list[BaseException] = []
    loop = client._loop
    thread = client._thread
    assert loop is not None
    assert thread is not None

    def stop_on_loop() -> None:
        try:
            client.loop_stop()
        except BaseException as exc:  # pragma: no cover - regression capture
            errors.append(exc)
        finally:
            done.set()

    loop.call_soon_threadsafe(stop_on_loop)
    assert done.wait(timeout=2.0)
    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert errors == []

    # Clear the now-stale references left by the self-stop path.
    client.loop_stop()
