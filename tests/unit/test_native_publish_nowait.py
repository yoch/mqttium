"""Loop-bound native publish admission and adapter-boundary contracts."""

from __future__ import annotations

import inspect

import pytest

import mqttium.compat.paho as paho_compat
from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState, QoS


def test_publish_nowait_requires_a_running_loop() -> None:
    client = AsyncClient()
    with pytest.raises(RuntimeError, match="event-loop thread"):
        client.publish_nowait("native/off-loop", b"x")


async def test_publish_nowait_registers_qos1_receipt() -> None:
    client = AsyncClient()
    client._engine.state = ConnectionState.CONNECTED
    receipt = client.publish_nowait("native/qos1", b"x", qos=1)
    assert receipt.qos is QoS.AT_LEAST_ONCE
    assert receipt.mid is not None
    assert client._pop_publish_receipt(receipt.mid) is receipt


async def test_publish_nowait_coalesces_async_effect_flush() -> None:
    client = AsyncClient(max_outbound_messages=512)
    client._engine.state = ConnectionState.CONNECTED
    seen: list[int | None] = []
    client.on_publish = lambda mid, _error: seen.append(mid)

    for _ in range(100):
        client.publish_nowait("native/qos0", b"x", qos=0)

    task = client._effect_flush_task
    assert task is not None
    assert not task.done()
    await task
    await client._callback_queue.join()
    assert seen == [None] * 100
    await client._shutdown_callback_worker(drain=False)


def test_paho_uses_the_async_client_adapter_boundary() -> None:
    source = inspect.getsource(paho_compat.Client)
    for forbidden in (
        "self._async._engine",
        "self._async._register_publish_receipt",
        "self._async._collect_effects_locked",
        "self._async._drain_effects_inline",
    ):
        assert forbidden not in source


def test_disconnect_metadata_boundary_is_private() -> None:
    assert not hasattr(AsyncClient, "last_disconnect")
    assert hasattr(AsyncClient, "_last_disconnect_info")
