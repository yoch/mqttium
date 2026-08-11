"""Live Mosquitto integration (requires broker on 127.0.0.1:11883)."""

from __future__ import annotations

import asyncio
import os
import socket

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion


def _broker_up() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 11883), timeout=0.5):
            return True
    except OSError:
        return False


_BROKER_AVAILABLE = _broker_up()
if os.environ.get("MQTTIUM_REQUIRE_BROKER") == "1" and not _BROKER_AVAILABLE:
    raise RuntimeError("MQTTIUM_REQUIRE_BROKER=1 but Mosquitto is not listening on :11883")

pytestmark = pytest.mark.skipif(not _BROKER_AVAILABLE, reason="Mosquitto not on :11883")


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
@pytest.mark.parametrize("qos", [0, 1, 2])
async def test_pubsub_roundtrip(protocol: MQTTProtocolVersion, qos: int) -> None:
    topic = f"mqttium/it/{int(protocol)}/{qos}"
    got: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()

    sub = AsyncClient(f"sub-{protocol}-{qos}", protocol=protocol)
    pub = AsyncClient(f"pub-{protocol}-{qos}", protocol=protocol)

    def on_message(msg) -> None:  # noqa: ANN001
        if not got.done():
            got.set_result(msg.payload)

    sub.on_message = on_message
    await sub.connect("127.0.0.1", 11883, timeout=5)
    await sub.subscribe(topic, qos=qos if qos else 0)
    await pub.connect("127.0.0.1", 11883, timeout=5)
    receipt = await pub.publish(topic, b"hello-it", qos=qos)
    if qos:
        await receipt.wait()
    payload = await asyncio.wait_for(got, timeout=5)
    assert payload == b"hello-it"
    await pub.disconnect()
    await sub.disconnect()
