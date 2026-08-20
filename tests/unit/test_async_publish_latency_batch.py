from __future__ import annotations

import pytest

from mqttium.api._writer import WritePump
from mqttium.api.async_client import AsyncClient
from mqttium.enums import ConnectionState


async def test_latency_batch_trigger_is_awaited_qos_only(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[WritePump] = []

    def record(pump: WritePump) -> bool:
        calls.append(pump)
        return False

    monkeypatch.setattr(WritePump, "_try_flush_latency_batch", record)

    awaited_qos1 = AsyncClient()
    awaited_qos1._engine.state = ConnectionState.CONNECTED
    await awaited_qos1.publish("latency/awaited", b"x", qos=1)
    assert calls == [awaited_qos1._write_pump]

    calls.clear()
    nowait_qos1 = AsyncClient()
    nowait_qos1._engine.state = ConnectionState.CONNECTED
    await nowait_qos1.publish("latency/nowait", b"x", qos=1, nowait=True)
    assert calls == []

    awaited_qos0 = AsyncClient()
    awaited_qos0._engine.state = ConnectionState.CONNECTED
    await awaited_qos0.publish("latency/qos0", b"x", qos=0)
    assert calls == []
