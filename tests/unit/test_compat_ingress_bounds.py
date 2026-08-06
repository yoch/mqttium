"""Hard bounds for the Paho façade's cross-thread publish handoff."""

from __future__ import annotations

import asyncio
import threading
from typing import Any, cast

import pytest

from mqttium.compat.paho import CallbackAPIVersion, Client, MQTT_ERR_QUEUE_SIZE
from mqttium.enums import ConnectionState


class _LoopStub:
    def __init__(self, *, fail_schedule: bool = False) -> None:
        self.calls: list[tuple[Any, tuple[Any, ...]]] = []
        self.fail_schedule = fail_schedule

    def is_running(self) -> bool:
        return True

    def call_soon_threadsafe(self, callback: Any, *args: Any) -> None:
        if self.fail_schedule:
            raise RuntimeError("loop stopped")
        self.calls.append((callback, args))

    def call_soon(self, callback: Any, *args: Any) -> None:
        self.calls.append((callback, args))

    def call_exception_handler(self, context: dict[str, Any]) -> None:
        raise AssertionError(context)


def _client_with_stub(**kwargs: Any) -> tuple[Client, _LoopStub]:
    client = Client(CallbackAPIVersion.VERSION2, client_id="bounded-ingress", **kwargs)
    client._async._engine.state = ConnectionState.CONNECTED
    loop = _LoopStub()
    client._loop = cast(asyncio.AbstractEventLoop, loop)
    client._finalize_publish_effects = lambda: client._async._engine.take_effects()
    return client, loop


def _drain_stub(loop: _LoopStub) -> None:
    while loop.calls:
        callback, args = loop.calls.pop(0)
        callback(*args)


def test_publish_handoff_is_hard_bounded_by_request_count() -> None:
    client, loop = _client_with_stub(
        max_pending_publish_requests=2,
        max_pending_publish_bytes=1_000,
    )

    accepted = [client.publish(f"count/{index}", b"x", qos=0) for index in range(2)]
    rejected = client.publish("count/rejected", b"x", qos=0)

    assert [info.rc for info in accepted] == [0, 0]
    assert rejected.rc == MQTT_ERR_QUEUE_SIZE
    assert client._pending_publish_requests == 2
    assert client._pending_publish_bytes == sum(len(f"count/{index}") + 1 for index in range(2))
    assert len(loop.calls) == 1

    _drain_stub(loop)
    assert client._pending_publish_requests == 0
    assert client._pending_publish_bytes == 0

    assert client.publish("count/after-drain", b"x", qos=0).rc == 0
    _drain_stub(loop)


def test_publish_handoff_is_hard_bounded_by_logical_bytes() -> None:
    client, loop = _client_with_stub(
        max_pending_publish_requests=10,
        max_pending_publish_bytes=5,
    )

    assert client.publish("a", b"1234", qos=0).rc == 0
    assert client._pending_publish_bytes == 5
    assert client.publish("b", b"x", qos=0).rc == MQTT_ERR_QUEUE_SIZE
    assert client._pending_publish_requests == 1
    assert client._pending_publish_bytes == 5

    _drain_stub(loop)
    assert client._pending_publish_requests == 0
    assert client._pending_publish_bytes == 0


def test_qosn_is_rejected_before_waiting_when_handoff_is_full() -> None:
    client, loop = _client_with_stub(
        max_pending_publish_requests=1,
        max_pending_publish_bytes=1_000,
    )

    assert client.publish("fill", b"x", qos=0).rc == 0
    rejected = client.publish("qos1", b"x", qos=1)

    assert rejected.rc == MQTT_ERR_QUEUE_SIZE
    assert rejected.mid is None
    assert client._pending_publish_requests == 1
    _drain_stub(loop)


def test_concurrent_publishers_cannot_exceed_the_handoff_limit() -> None:
    limit = 64
    client, loop = _client_with_stub(
        max_pending_publish_requests=limit,
        max_pending_publish_bytes=1_000_000,
    )
    workers = 8
    per_worker = 40
    barrier = threading.Barrier(workers)
    results: list[int] = []
    result_lock = threading.Lock()

    def publish_many(worker: int) -> None:
        barrier.wait()
        local = [
            client.publish(f"concurrent/{worker}/{index}", b"x", qos=0).rc
            for index in range(per_worker)
        ]
        with result_lock:
            results.extend(local)

    threads = [threading.Thread(target=publish_many, args=(worker,)) for worker in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5.0)

    assert all(not thread.is_alive() for thread in threads)
    assert results.count(0) == limit
    assert results.count(MQTT_ERR_QUEUE_SIZE) == workers * per_worker - limit
    assert client._pending_publish_requests == limit
    assert len(loop.calls) == 1

    _drain_stub(loop)
    assert client._pending_publish_requests == 0
    assert client._pending_publish_bytes == 0


def test_failed_loop_schedule_releases_the_reserved_request() -> None:
    client = Client(
        CallbackAPIVersion.VERSION2,
        client_id="schedule-failure",
        max_pending_publish_requests=1,
        max_pending_publish_bytes=100,
    )
    client._async._engine.state = ConnectionState.CONNECTED
    client._loop = cast(asyncio.AbstractEventLoop, _LoopStub(fail_schedule=True))

    with pytest.raises(RuntimeError, match="loop stopped"):
        client.publish("failure", b"payload", qos=0)

    assert client._pending_publish_requests == 0
    assert client._pending_publish_bytes == 0
    assert client._publish_pending.empty()
    assert client._publish_spillover is None


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("max_pending_publish_requests", 0),
        ("max_pending_publish_requests", -1),
        ("max_pending_publish_bytes", 0),
        ("max_pending_publish_bytes", -1),
    ],
)
def test_publish_handoff_limits_must_be_positive(name: str, value: int) -> None:
    with pytest.raises(ValueError, match=name):
        Client(CallbackAPIVersion.VERSION2, **{name: value})
